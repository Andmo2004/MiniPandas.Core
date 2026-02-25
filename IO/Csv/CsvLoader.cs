using System;
using System.Collections.Generic;
using System.IO;
using MiniPandas.Core;
using MiniPandas.Core.IO;
using MiniPandas.Core.IO.Csv;

namespace MiniPandas.Core.IO.Csv
{
    /// <summary>
    /// Loader de ficheros CSV. Implementa IDataLoader para ser intercambiable
    /// con ExcelLoader y JsonLoader sin conocer el formato en el llamador.
    ///
    /// PARSER CSV PROPIO:
    ///   No usa librerías externas. Implementa RFC 4180 con las extensiones habituales:
    ///     - Campos entre comillas pueden contener el separador y saltos de línea.
    ///     - Comilla doble ("") dentro de un campo entrecomillado = comilla literal.
    ///     - Líneas completamente vacías se descartan (como ExcelLoader).
    ///     - BOM UTF-8 se ignora automáticamente.
    ///
    /// INFERENCIA DE TIPOS:
    ///   Delega en SchemaInference, igual que ExcelLoader.
    ///   El umbral de columnas categóricas viene de CsvOptions.CategoricalThreshold.
    ///
    /// USO:
    ///   // Coma (por defecto):
    ///   var df = new CsvLoader().Load("datos.csv");
    ///
    ///   // Punto y coma (Europa):
    ///   var df = new CsvLoader().Load("datos.csv", CsvOptions.European);
    ///
    ///   // Opciones a medida:
    ///   var df = new CsvLoader().Load("datos.csv", new CsvOptions(delimiter: '\t'));
    /// </summary>
    public sealed class CsvLoader : IDataLoader
    {
        /// <summary>
        /// Carga un fichero CSV y devuelve un DataFrame.
        /// </summary>
        /// <param name="path">Ruta al fichero CSV.</param>
        /// <param name="options">
        /// Opciones de carga. Acepta LoadOptions genérico o CsvOptions específico.
        /// Si es null se usa CsvOptions.Default (coma, UTF-8, con cabecera).
        /// </param>
        public DataFrame Load(string path, LoadOptions options = null)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be null or empty.", nameof(path));
            if (!File.Exists(path))
                throw new FileNotFoundException("CSV file not found.", path);

            // Resolver opciones: acepta CsvOptions directamente o LoadOptions genérico
            var csvOpts = options as CsvOptions;
            if (csvOpts == null && options != null)
                csvOpts = new CsvOptions(
                    hasHeader: options.HasHeader,
                    categoricalThreshold: options.CategoricalThreshold);
            var opts = csvOpts ?? CsvOptions.Default;

            // ── 1. Leer y parsear todas las líneas ────────────────────────────
            var rawRows = new List<object[]>();
            string[] columnNames = null;

            using (var reader = new StreamReader(path, opts.Encoding, detectEncodingFromByteOrderMarks: true))
            {
                string line;
                bool firstDataLine = true;

                while ((line = reader.ReadLine()) != null)
                {
                    // Descartar líneas vacías
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    var fields = ParseCsvLine(line, opts.Delimiter, opts.QuoteChar);

                    if (firstDataLine && opts.HasHeader)
                    {
                        // Primera línea como cabecera
                        columnNames = NormalizeHeaders(fields);
                        firstDataLine = false;
                        continue;
                    }

                    if (firstDataLine)
                    {
                        // Sin cabecera: generar nombres Column0, Column1, ...
                        columnNames = GenerateColumnNames(fields.Length);
                        firstDataLine = false;
                    }

                    // Ajustar si la fila tiene menos columnas que la cabecera (filas cortas)
                    var row = new object[columnNames.Length];
                    int copyLen = Math.Min(fields.Length, columnNames.Length);

                    for (int i = 0; i < copyLen; i++)
                    {
                        string field = fields[i];
                        row[i] = (opts.TreatEmptyAsNull && string.IsNullOrEmpty(field))
                            ? null
                            : field;
                    }
                    // Las posiciones [copyLen..columnNames.Length) quedan null (celdas nulas)

                    rawRows.Add(row);
                }
            }

            if (columnNames == null)
                throw new InvalidDataException("The CSV file is empty or contains only a header.");

            // ── 2. Delegar inferencia de tipos en SchemaInference ─────────────
            var df = new DataFrame(rawRows.Count);
            foreach (var column in Core.SchemaInference.InferColumns(columnNames, rawRows, opts))
                df.AddColumn(column);

            return df;
        }

        // ── Parser CSV (RFC 4180) ─────────────────────────────────────────────

        /// <summary>
        /// Parsea una línea CSV respetando campos entrecomillados.
        /// Soporta:
        ///   - Campos con el separador dentro de comillas: a,"b,c",d → ["a","b,c","d"]
        ///   - Comillas dobles como escape: a,"b""c",d → ["a","b\"c","d"]
        ///   - Campos vacíos: a,,c → ["a","","c"]
        ///
        /// LIMITACIÓN: no soporta campos multilínea (newlines dentro de comillas).
        /// Para ese caso usar una librería como CsvHelper.
        /// </summary>
        private static string[] ParseCsvLine(string line, char delimiter, char quote)
        {
            var fields = new List<string>();
            int pos = 0;
            int len = line.Length;

            while (pos <= len)
            {
                if (pos == len)
                {
                    // Línea termina en separador → campo vacío final
                    fields.Add(string.Empty);
                    break;
                }

                if (line[pos] == quote)
                {
                    // ── Campo entrecomillado ──────────────────────────────────
                    pos++;   // saltar la comilla de apertura
                    var sb = new System.Text.StringBuilder();

                    while (pos < len)
                    {
                        char c = line[pos];

                        if (c == quote)
                        {
                            // Comilla: ¿escape ("") o cierre?
                            if (pos + 1 < len && line[pos + 1] == quote)
                            {
                                sb.Append(quote);   // comilla escapada
                                pos += 2;
                            }
                            else
                            {
                                pos++;   // comilla de cierre
                                break;
                            }
                        }
                        else
                        {
                            sb.Append(c);
                            pos++;
                        }
                    }

                    fields.Add(sb.ToString());

                    // Avanzar hasta el siguiente separador o fin de línea
                    if (pos < len && line[pos] == delimiter) pos++;
                }
                else
                {
                    // ── Campo sin comillas ────────────────────────────────────
                    int start = pos;
                    while (pos < len && line[pos] != delimiter)
                        pos++;

                    fields.Add(line.Substring(start, pos - start));
                    if (pos < len) pos++;   // saltar el separador
                }
            }

            // Edge case: línea vacía → un campo vacío
            if (fields.Count == 0) fields.Add(string.Empty);

            return fields.ToArray();
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private static string[] NormalizeHeaders(string[] raw)
        {
            var names = new string[raw.Length];
            var seen = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < raw.Length; i++)
            {
                string name = string.IsNullOrWhiteSpace(raw[i])
                    ? $"Column{i}"
                    : raw[i].Trim();

                // Desduplicar: si "precio" ya existe, el segundo se llama "precio_1"
                if (seen.TryGetValue(name, out int count))
                {
                    seen[name] = count + 1;
                    name = $"{name}_{count}";
                }
                else
                {
                    seen[name] = 1;
                }

                names[i] = name;
            }

            return names;
        }

        private static string[] GenerateColumnNames(int count)
        {
            var names = new string[count];
            for (int i = 0; i < count; i++)
                names[i] = $"Column{i}";
            return names;
        }
    }
}