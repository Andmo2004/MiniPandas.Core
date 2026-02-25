// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Linq;
using System.Globalization;
using Newtonsoft.Json;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.IO
{
    public static class JsonExporter
    {
        /// <summary>
        /// Exporta el DataFrame a un archivo JSON.
        /// </summary>
        public static void Write(DataFrame df, string path, JsonOrientation orientation = JsonOrientation.Records)
        {
            if (df == null) throw new ArgumentNullException(nameof(df));
            if (path == null) throw new ArgumentNullException(nameof(path));

            using (var sw = new StreamWriter(path, append: false, encoding: System.Text.Encoding.UTF8))
            using (var writer = new JsonTextWriter(sw))
            {
                writer.Formatting = Formatting.Indented;

                // ── Fix: switch explícito en lugar del if/else que tragaba Split ──
                switch (orientation)
                {
                    case JsonOrientation.Records:
                        WriteRecords(df, writer);
                        break;
                    case JsonOrientation.Columns:
                        WriteColumns(df, writer);
                        break;
                    case JsonOrientation.Split:
                        WriteSplit(df, writer);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(
                            nameof(orientation),
                            $"Unknown JsonOrientation value: {orientation}.");
                }
            }
        }

        // ── Records: [{a:1, b:2}, {a:3, b:4}] ───────────────────────────────
        private static void WriteRecords(DataFrame df, JsonTextWriter writer)
        {
            writer.WriteStartArray();

            for (int row = 0; row < df.RowCount; row++)
            {
                writer.WriteStartObject();
                foreach (var column in df.Columns)
                {
                    writer.WritePropertyName(column.Name);
                    WriteValue(writer, column, row);
                }
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        // ── Columns: {a:[1,3], b:[2,4]} ──────────────────────────────────────
        private static void WriteColumns(DataFrame df, JsonTextWriter writer)
        {
            writer.WriteStartObject();

            foreach (var column in df.Columns)
            {
                writer.WritePropertyName(column.Name);
                writer.WriteStartArray();

                for (int row = 0; row < df.RowCount; row++)
                    WriteValue(writer, column, row);

                writer.WriteEndArray();
            }

            writer.WriteEndObject();
        }

        // ── Split: {columns:[...], data:[[v1,v2],[v3,v4]]} ───────────────────
        private static void WriteSplit(DataFrame df, JsonTextWriter writer)
        {
            writer.WriteStartObject();

            // "columns": ["col1", "col2", ...]
            writer.WritePropertyName("columns");
            writer.WriteStartArray();
            foreach (var col in df.Columns)
                writer.WriteValue(col.Name);
            writer.WriteEndArray();

            // "data": [[v1, v2], [v3, v4], ...]
            writer.WritePropertyName("data");
            writer.WriteStartArray();

            var columnList = df.Columns.ToList();   // evitar re-enumerar en cada fila

            for (int row = 0; row < df.RowCount; row++)
            {
                writer.WriteStartArray();
                foreach (var col in columnList)
                    WriteValue(writer, col, row);
                writer.WriteEndArray();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        // ── Escritura de una celda individual ─────────────────────────────────
        // Dispatch centralizado por tipo. Cuando añadas nuevos DataColumn<T>
        // (int, bool, etc.), solo tienes que tocar este método.
        private static void WriteValue(JsonTextWriter writer, BaseColumn column, int row)
        {
            if (column.IsNull(row))
            {
                writer.WriteNull();
                return;
            }

            if (column is DataColumn<double> dcDouble)
            {
                var val = dcDouble.GetRawValue(row);
                // NaN e Infinity no son JSON válido: los escribimos como null (igual que pandas)
                if (double.IsNaN(val) || double.IsInfinity(val))
                    writer.WriteNull();
                else
                    writer.WriteValue(val);
            }
            else if (column is DataColumn<DateTime> dcDateTime)
            {
                // ISO 8601, el estándar de pandas para fechas en JSON
                writer.WriteValue(dcDateTime.GetRawValue(row).ToString("o", CultureInfo.InvariantCulture));
            }
            else if (column is DataColumn<bool> dcBool)
            {
                writer.WriteValue(dcBool.GetRawValue(row));
            }
            else if (column is DataColumn<int> dcInt)
            {
                writer.WriteValue(dcInt.GetRawValue(row));
            }
            else if (column is StringColumn sc)
            {
                writer.WriteValue(sc[row]);
            }
            else if (column is CategoricalColumn cc)
            {
                // CategoricalColumn descodifica el código a string automáticamente
                writer.WriteValue(cc[row]);
            }
            else
            {
                // Tipo desconocido: fallback seguro en lugar de silencio
                writer.WriteValue($"[unsupported:{column.GetType().Name}]");
            }
        }
    }

    public enum JsonOrientation
    {
        /// <summary>
        /// [{col1: v1, col2: v2}, ...]
        /// Legible, compatible con pandas read_json default.
        /// Útil para consumo directo por humanos o APIs REST simples.
        /// Penaliza en tamaño: repite el nombre de cada columna en cada fila.
        /// </summary>
        Records,

        /// <summary>
        /// {col1: [v1, v2, ...], col2: [v1, v2, ...]}
        /// Óptimo para datos web: Chart.js, D3.js, dashboards.
        /// Un array por columna, ideal para graficar series directamente.
        /// </summary>
        Columns,

        /// <summary>
        /// {columns: ["col1","col2"], data: [[v1,v2],[v3,v4]]}
        /// Óptimo para bulk insert a SQL: los nombres de columna aparecen
        /// una sola vez y cada fila es un array posicional.
        /// Mapea directamente a SqlBulkCopy o parámetros de Dapper.
        /// Es el formato más compacto de los tres.
        /// </summary>
        Split
    }
}