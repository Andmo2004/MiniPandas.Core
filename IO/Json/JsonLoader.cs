// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using MiniPandas.Core;
using MiniPandas.Core.IO;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.IO.Json
{
    /// <summary>
    /// Loader de ficheros JSON. Implementa IDataLoader.
    /// Soporta las mismas tres orientaciones que JsonExporter:
    ///   Records → [{col:val}, ...]
    ///   Columns → {col:[val,...]}
    ///   Split   → {columns:[...], data:[[val,...],...]}
    ///
    /// AUTODETECCIÓN:
    ///   Si Orientation == AutoDetectOrientation (por defecto), el loader inspecciona
    ///   el token raíz del JSON para inferir la orientación:
    ///     '[' → Records
    ///     '{' con claves "columns" y "data" → Split
    ///     '{' en otro caso → Columns
    ///
    /// INFERENCIA DE TIPOS:
    ///   Los valores se recogen como object y se delegan a SchemaInference,
    ///   igual que CsvLoader y ExcelLoader.
    ///
    /// USO:
    ///   var df = new JsonLoader().Load("datos.json");
    ///   var df = new JsonLoader().Load("datos.json", JsonOptions.Records);
    ///   var df = new JsonLoader().Load("datos.json", new JsonOptions(JsonOrientation.Split));
    /// </summary>
    public sealed class JsonLoader : IDataLoader
    {
        public DataFrame Load(string path, LoadOptions options = null)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be null or empty.", nameof(path));
            if (!File.Exists(path))
                throw new FileNotFoundException("JSON file not found.", path);

            var jsonOpts = options as JsonOptions;
            if (jsonOpts == null && options != null)
                jsonOpts = new JsonOptions(
                    categoricalThreshold: options.CategoricalThreshold);
            var opts = jsonOpts ?? JsonOptions.AutoDetect;

            string json = File.ReadAllText(path, System.Text.Encoding.UTF8);

            if (string.IsNullOrWhiteSpace(json))
                throw new InvalidDataException("The JSON file is empty.");

            JToken root;
            try
            {
                root = JToken.Parse(json);
            }
            catch (JsonException ex)
            {
                throw new InvalidDataException(
                    string.Format("Invalid JSON in '{0}': {1}", path, ex.Message), ex);
            }

            // ── Detectar o usar orientación indicada ──────────────────────────
            JsonOrientation orientation = opts.Orientation == JsonOptions.AutoDetectOrientation
                ? DetectOrientation(root, path)
                : opts.Orientation;

            // ── Parsear según orientación ─────────────────────────────────────
            string[] columnNames;
            List<object[]> rawRows;

            switch (orientation)
            {
                case JsonOrientation.Records:
                    ParseRecords(root, path, out columnNames, out rawRows);
                    break;
                case JsonOrientation.Columns:
                    ParseColumns(root, path, out columnNames, out rawRows);
                    break;
                case JsonOrientation.Split:
                    ParseSplit(root, path, out columnNames, out rawRows);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        nameof(orientation),
                        string.Format("Unknown JsonOrientation: {0}.", orientation));
            }

            if (rawRows.Count == 0)
            {
                // JSON válido pero sin filas de datos: DataFrame vacío con columnas
                var empty = new DataFrame(0);
                foreach (var name in columnNames)
                    empty.AddColumn(new MiniPandas.Core.Columns.StringColumn(name, 0));
                return empty;
            }

            // ── Inferencia de tipos ───────────────────────────────────────────
            var df = new DataFrame(rawRows.Count);
            foreach (var column in Core.SchemaInference.InferColumns(columnNames, rawRows, opts))
                df.AddColumn(column);

            return df;
        }

        // ── Detección de orientación ──────────────────────────────────────────

        private static JsonOrientation DetectOrientation(JToken root, string path)
        {
            if (root is JArray)
                return JsonOrientation.Records;

            var obj = root as JObject;
            if (obj != null)
            {
                if (obj.ContainsKey("columns") && obj.ContainsKey("data"))
                    return JsonOrientation.Split;

                return JsonOrientation.Columns;
            }

            throw new InvalidDataException(
                string.Format(
                    "Cannot detect JSON orientation in '{0}'. " +
                    "Expected array ([...]) or object ({{...}}) at root.", path));
        }

        // ── Parsers por orientación ───────────────────────────────────────────

        /// <summary>
        /// Records: [{col1:v1, col2:v2}, {col1:v3, col2:v4}]
        /// Las columnas se infieren del primer objeto no nulo.
        /// </summary>
        private static void ParseRecords(
            JToken root, string path,
            out string[] columnNames, out List<object[]> rawRows)
        {
            var array = root as JArray;
            if (array == null)
                throw new InvalidDataException(
                    string.Format(
                        "Expected JSON array for Records orientation in '{0}'.", path));

            if (array.Count == 0)
            {
                columnNames = new string[0];
                rawRows = new List<object[]>();
                return;
            }

            // Inferir columnas del primer objeto no nulo
            var firstObj = FindFirstObject(array, path);
            var colIndex = BuildColumnIndex(firstObj);

            columnNames = new string[colIndex.Count];
            foreach (var kvp in colIndex)
                columnNames[kvp.Value] = kvp.Key;

            rawRows = new List<object[]>(array.Count);

            foreach (var token in array)
            {
                if (token.Type == JTokenType.Null) continue;

                var record = token as JObject;
                if (record == null)
                    throw new InvalidDataException(
                        string.Format(
                            "Expected JSON objects inside array for Records orientation in '{0}'.", path));

                var row = new object[columnNames.Length];
                foreach (var prop in record.Properties())
                {
                    int idx;
                    if (colIndex.TryGetValue(prop.Name, out idx))
                        row[idx] = TokenToObject(prop.Value);
                }
                rawRows.Add(row);
            }
        }

        /// <summary>
        /// Columns: {col1:[v1,v3], col2:[v2,v4]}
        /// Todas las arrays deben tener la misma longitud.
        /// </summary>
        private static void ParseColumns(
            JToken root, string path,
            out string[] columnNames, out List<object[]> rawRows)
        {
            var obj = root as JObject;
            if (obj == null)
                throw new InvalidDataException(
                    string.Format(
                        "Expected JSON object for Columns orientation in '{0}'.", path));

            var props = new List<JProperty>(obj.Properties());

            if (props.Count == 0)
            {
                columnNames = new string[0];
                rawRows = new List<object[]>();
                return;
            }

            columnNames = new string[props.Count];
            var arrays = new JArray[props.Count];
            int rowCount = -1;

            for (int c = 0; c < props.Count; c++)
            {
                columnNames[c] = props[c].Name;

                var arr = props[c].Value as JArray;
                if (arr == null)
                    throw new InvalidDataException(
                        string.Format(
                            "Column '{0}' is not an array in Columns-oriented JSON '{1}'.",
                            props[c].Name, path));

                if (rowCount == -1)
                    rowCount = arr.Count;
                else if (arr.Count != rowCount)
                    throw new InvalidDataException(
                        string.Format(
                            "Column '{0}' has {1} values but expected {2} in '{3}'.",
                            props[c].Name, arr.Count, rowCount, path));

                arrays[c] = arr;
            }

            if (rowCount <= 0)
            {
                rawRows = new List<object[]>();
                return;
            }

            rawRows = new List<object[]>(rowCount);
            for (int r = 0; r < rowCount; r++)
            {
                var row = new object[props.Count];
                for (int c = 0; c < props.Count; c++)
                    row[c] = TokenToObject(arrays[c][r]);
                rawRows.Add(row);
            }
        }

        /// <summary>
        /// Split: {columns:["col1","col2"], data:[[v1,v2],[v3,v4]]}
        /// </summary>
        private static void ParseSplit(
            JToken root, string path,
            out string[] columnNames, out List<object[]> rawRows)
        {
            var obj = root as JObject;
            if (obj == null)
                throw new InvalidDataException(
                    string.Format(
                        "Expected JSON object for Split orientation in '{0}'.", path));

            var colsToken = obj["columns"] as JArray;
            if (colsToken == null)
                throw new InvalidDataException(
                    string.Format(
                        "Missing or invalid 'columns' array in Split-oriented JSON '{0}'.", path));

            var dataToken = obj["data"] as JArray;
            if (dataToken == null)
                throw new InvalidDataException(
                    string.Format(
                        "Missing or invalid 'data' array in Split-oriented JSON '{0}'.", path));

            columnNames = new string[colsToken.Count];
            for (int c = 0; c < colsToken.Count; c++)
                columnNames[c] = colsToken[c].Value<string>() ?? string.Format("Column{0}", c);

            rawRows = new List<object[]>(dataToken.Count);

            foreach (var rowToken in dataToken)
            {
                var rowArr = rowToken as JArray;
                if (rowArr == null)
                    throw new InvalidDataException(
                        string.Format(
                            "Each row in 'data' must be an array in Split-oriented JSON '{0}'.", path));

                var row = new object[columnNames.Length];
                int copyLen = Math.Min(rowArr.Count, columnNames.Length);
                for (int c = 0; c < copyLen; c++)
                    row[c] = TokenToObject(rowArr[c]);

                rawRows.Add(row);
            }
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private static JObject FindFirstObject(JArray array, string path)
        {
            foreach (var token in array)
            {
                var obj = token as JObject;
                if (obj != null) return obj;
            }

            throw new InvalidDataException(
                string.Format("No valid JSON objects found in array in '{0}'.", path));
        }

        private static Dictionary<string, int> BuildColumnIndex(JObject obj)
        {
            var index = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var prop in obj.Properties())
            {
                if (!index.ContainsKey(prop.Name))
                    index[prop.Name] = index.Count;
            }
            return index;
        }

        /// <summary>
        /// Convierte un JToken a un object que SchemaInference pueda clasificar.
        /// null → null, enteros → long, decimales → double, bool → bool, string → string.
        /// </summary>
        private static object TokenToObject(JToken token)
        {
            if (token == null || token.Type == JTokenType.Null)
                return null;

            switch (token.Type)
            {
                case JTokenType.Integer: return token.Value<long>();
                case JTokenType.Float: return token.Value<double>();
                case JTokenType.Boolean: return token.Value<bool>();
                case JTokenType.Date: return token.Value<DateTime>();
                case JTokenType.String: return token.Value<string>();
                default: return token.ToString(Formatting.None);
            }
        }
    }
}