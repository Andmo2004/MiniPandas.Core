// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Globalization;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.IO.Csv
{
    /// <summary>
    /// Exportador de DataFrames a ficheros CSV.
    ///
    /// FORMATO DE SALIDA:
    ///   - Primera fila: nombres de columna.
    ///   - Resto: una fila por registro.
    ///   - Celdas nulas → campo vacío ("").
    ///   - Campos que contienen el separador, comillas o saltos de línea
    ///     se envuelven entre comillas (RFC 4180).
    ///   - Fechas en ISO 8601 (yyyy-MM-dd) para máxima portabilidad.
    ///   - Doubles en InvariantCulture (punto decimal) independientemente
    ///     de la cultura del sistema.
    ///
    /// USO:
    ///   CsvExporter.Write(df, "salida.csv");
    ///   CsvExporter.Write(df, "salida.csv", CsvOptions.European);
    /// </summary>
    public static class CsvExporter
    {
        /// <summary>
        /// Escribe un DataFrame en un fichero CSV.
        /// </summary>
        /// <param name="df">DataFrame a exportar.</param>
        /// <param name="path">Ruta del fichero de salida.</param>
        /// <param name="options">
        /// Opciones de exportación. Si es null se usa CsvOptions.Default (coma, UTF-8).
        /// </param>
        public static void Write(DataFrame df, string path, CsvOptions options = null)
        {
            if (df == null) throw new ArgumentNullException(nameof(df));
            if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("Path cannot be null or empty.", nameof(path));

            var opts = options ?? CsvOptions.Default;

            using (var sw = new StreamWriter(path, append: false, encoding: opts.Encoding))
            {
                var columns = new System.Collections.Generic.List<BaseColumn>(df.Columns);
                char delimiter = opts.Delimiter;
                char quote = opts.QuoteChar;

                // ── Cabecera ──────────────────────────────────────────────────
                for (int c = 0; c < columns.Count; c++)
                {
                    if (c > 0) sw.Write(delimiter);
                    sw.Write(EscapeField(columns[c].Name, delimiter, quote));
                }
                sw.WriteLine();

                // ── Filas ─────────────────────────────────────────────────────
                for (int row = 0; row < df.RowCount; row++)
                {
                    for (int c = 0; c < columns.Count; c++)
                    {
                        if (c > 0) sw.Write(delimiter);
                        sw.Write(FormatCell(columns[c], row, delimiter, quote));
                    }
                    sw.WriteLine();
                }
            }
        }

        // ── Formateo de celda ─────────────────────────────────────────────────

        private static string FormatCell(BaseColumn column, int row, char delimiter, char quote)
        {
            if (column.IsNull(row)) return string.Empty;

            string value;

            if (column is DataColumn<double> dcD)
            {
                double v = dcD.GetRawValue(row);
                // NaN e Infinity → vacío (como pandas)
                value = double.IsNaN(v) || double.IsInfinity(v)
                    ? string.Empty
                    : v.ToString("G", CultureInfo.InvariantCulture);
            }
            else if (column is DataColumn<int> dcI)
            {
                value = dcI.GetRawValue(row).ToString(CultureInfo.InvariantCulture);
            }
            else if (column is DataColumn<DateTime> dcDt)
            {
                // ISO 8601 fecha+hora solo si tiene componente de tiempo, solo fecha si no
                var dt = dcDt.GetRawValue(row);
                value = dt.TimeOfDay == TimeSpan.Zero
                    ? dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                    : dt.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture);
            }
            else if (column is DataColumn<bool> dcB)
            {
                // true/false en minúsculas (compatible con Python/pandas)
                value = dcB.GetRawValue(row) ? "true" : "false";
            }
            else if (column is StringColumn sc)
            {
                value = sc[row] ?? string.Empty;
            }
            else if (column is CategoricalColumn cc)
            {
                value = cc[row] ?? string.Empty;
            }
            else
            {
                // Fallback seguro para tipos futuros
                value = column.GetBoxed(row)?.ToString() ?? string.Empty;
            }

            return EscapeField(value, delimiter, quote);
        }

        // ── Escape RFC 4180 ───────────────────────────────────────────────────

        /// <summary>
        /// Envuelve el campo entre comillas si contiene el separador, comillas
        /// o saltos de línea. Las comillas dentro del campo se duplican ("").
        /// Si el campo no necesita escape, se devuelve tal cual (sin asignaciones extra).
        /// </summary>
        private static string EscapeField(string value, char delimiter, char quote)
        {
            if (value == null) return string.Empty;

            bool needsQuoting = value.IndexOf(delimiter) >= 0
                             || value.IndexOf(quote) >= 0
                             || value.IndexOf('\n') >= 0
                             || value.IndexOf('\r') >= 0;

            if (!needsQuoting) return value;

            // Duplicar las comillas y envolver
            return quote
                + value.Replace(quote.ToString(), new string(quote, 2))
                + quote;
        }
    }
}