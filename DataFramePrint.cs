// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core
{
    public static class DataFrameExtensions
    {
        // ── Constantes de configuración ───────────────────────────────────────

        private const int DefaultMaxRows = 10;
        private const int DefaultMaxColWidth = 12;
        private const int IndexColWidth = 6;
        private const string Ellipsis = "...";
        private const string NullLabel = "NaN";

        // ── API pública ───────────────────────────────────────────────────────

        /// <summary>
        /// Imprime el DataFrame en consola con formato tabular estilo pandas.
        ///
        /// Ejemplo de salida:
        ///
        ///    ciudad        precio    ventas   activo
        ///    ──────────────────────────────────────
        /// 0  Madrid         10.50       120     True
        /// 1  Barcelona      20.00       340     True
        /// 2  Sevilla          NaN        85    False
        /// 3  Valencia       15.75       210     True
        ///
        /// [4 rows × 4 columns]
        ///
        /// </summary>
        /// <param name="maxRows">
        /// Nº máximo de filas a mostrar. Si el DataFrame tiene más, se muestran
        /// las primeras y las últimas (cabeza + cola), igual que pandas.
        /// 0 = mostrar todas.
        /// </param>
        /// <param name="maxColWidth">
        /// Ancho máximo de cada columna (caracteres). Los valores más largos
        /// se truncan con "…".
        /// </param>
        public static void Print(this DataFrame df, int maxRows = DefaultMaxRows, int maxColWidth = DefaultMaxColWidth)
        {
            Console.WriteLine(df.ToDisplayString(maxRows, maxColWidth));
        }

        /// <summary>
        /// Versión que escribe en cualquier writer (útil para tests y logging).
        /// </summary>
        public static void Print(this DataFrame df, System.IO.TextWriter writer, int maxRows = DefaultMaxRows, int maxColWidth = DefaultMaxColWidth)
        {
            writer.WriteLine(df.ToDisplayString(maxRows, maxColWidth));
        }

        /// <summary>
        /// Devuelve la representación tabular como string (sin imprimirla).
        /// Equivalente al __repr__ de pandas.
        /// </summary>
        public static string ToDisplayString(this DataFrame df, int maxRows = DefaultMaxRows, int maxColWidth = DefaultMaxColWidth)
        {
            if (df.RowCount == 0 || df.ColumnCount == 0)
                return BuildEmptyMessage(df);

            // ── 1. Determinar qué filas mostrar ───────────────────────────────
            int[] rowIndices = GetRowIndices(df, maxRows, out bool truncated);

            // ── 2. Calcular anchos de columna ─────────────────────────────────
            int[] colWidths = ComputeColumnWidths(df, rowIndices, maxColWidth);
            string[] colNames = df.ColumnNames.ToArray();

            // ── 3. Construir la salida ─────────────────────────────────────────
            var sb = new StringBuilder();

            AppendHeader(df, sb, colNames, colWidths);
            AppendSeparator(sb, colWidths);

            if (truncated)
            {
                int headCount = maxRows / 2;
                int tailCount = maxRows - headCount;

                for (int i = 0; i < headCount; i++)
                    AppendRow(df, sb, rowIndices[i], colNames, colWidths);

                AppendEllipsisRow(sb, colWidths);

                for (int i = headCount; i < headCount + tailCount; i++)
                    AppendRow(df, sb, rowIndices[i], colNames, colWidths);
            }
            else
            {
                foreach (int idx in rowIndices)
                    AppendRow(df, sb, idx, colNames, colWidths);
            }

            sb.AppendLine();
            sb.Append($"[{df.RowCount} rows × {df.ColumnCount} columns]");

            AppendDtypes(df, sb, colNames);

            return sb.ToString();
        }

        // ── Helpers privados ──────────────────────────────────────────────────

        private static string BuildEmptyMessage(DataFrame df)
        {
            if (df.ColumnCount == 0)
                return "Empty DataFrame\nColumns: []\n[0 rows × 0 columns]";

            var cols = string.Join(", ", df.ColumnNames);
            return $"Empty DataFrame\nColumns: [{cols}]\n[0 rows × {df.ColumnCount} columns]";
        }

        private static int[] GetRowIndices(DataFrame df, int maxRows, out bool truncated)
        {
            if (maxRows <= 0 || df.RowCount <= maxRows)
            {
                truncated = false;
                return Enumerable.Range(0, df.RowCount).ToArray();
            }

            truncated = true;
            int head = maxRows / 2;
            int tail = maxRows - head;

            var indices = new int[head + tail];
            for (int i = 0; i < head; i++)
                indices[i] = i;
            for (int i = 0; i < tail; i++)
                indices[head + i] = df.RowCount - tail + i;

            return indices;
        }

        private static int[] ComputeColumnWidths(DataFrame df, int[] rowIndices, int maxColWidth)
        {
            string[] colNames = df.ColumnNames.ToArray();
            int[] widths = new int[colNames.Length];

            for (int c = 0; c < colNames.Length; c++)
            {
                int w = Math.Min(colNames[c].Length, maxColWidth);
                BaseColumn col = df[colNames[c]];

                foreach (int r in rowIndices)
                {
                    string cell = FormatCell(col, r);
                    w = Math.Max(w, Math.Min(cell.Length, maxColWidth));
                }

                widths[c] = Math.Max(w, Ellipsis.Length);
            }

            return widths;
        }

        private static void AppendHeader(DataFrame df, StringBuilder sb, string[] colNames, int[] colWidths)
        {
            sb.Append(new string(' ', IndexColWidth + 1));

            for (int c = 0; c < colNames.Length; c++)
            {
                string name = Truncate(colNames[c], colWidths[c]);

                if (IsNumericColumn(df[colNames[c]]))
                    sb.Append(name.PadLeft(colWidths[c]));
                else
                    sb.Append(name.PadRight(colWidths[c]));

                if (c < colNames.Length - 1) sb.Append("  ");
            }
            sb.AppendLine();
        }

        private static void AppendSeparator(StringBuilder sb, int[] colWidths)
        {
            int totalWidth = IndexColWidth + 1
                + colWidths.Sum()
                + (colWidths.Length - 1) * 2;

            sb.AppendLine(new string('─', totalWidth));
        }

        private static void AppendRow(DataFrame df, StringBuilder sb, int rowIdx, string[] colNames, int[] colWidths)
        {
            sb.Append(rowIdx.ToString().PadLeft(IndexColWidth));
            sb.Append(' ');

            for (int c = 0; c < colNames.Length; c++)
            {
                BaseColumn col = df[colNames[c]];
                string cell = Truncate(FormatCell(col, rowIdx), colWidths[c]);

                if (IsNumericColumn(col))
                    sb.Append(cell.PadLeft(colWidths[c]));
                else
                    sb.Append(cell.PadRight(colWidths[c]));

                if (c < colNames.Length - 1) sb.Append("  ");
            }
            sb.AppendLine();
        }

        private static void AppendEllipsisRow(StringBuilder sb, int[] colWidths)
        {
            sb.Append(Ellipsis.PadLeft(IndexColWidth));
            sb.Append(' ');

            for (int c = 0; c < colWidths.Length; c++)
            {
                sb.Append(Ellipsis.PadLeft(colWidths[c]));
                if (c < colWidths.Length - 1) sb.Append("  ");
            }
            sb.AppendLine();
        }

        private static void AppendDtypes(DataFrame df, StringBuilder sb, string[] colNames)
        {
            sb.AppendLine();
            sb.AppendLine("dtypes:");
            foreach (var name in colNames)
            {
                string dtype = GetDtypeLabel(df[name]);
                sb.AppendLine($"  {name}: {dtype}");
            }
        }

        // ── Formateo de celdas ────────────────────────────────────────────────

        private static string FormatCell(BaseColumn col, int row)
        {
            if (col.IsNull(row)) return NullLabel;

            object val = col.GetBoxed(row);

            if (val is double d)
                return d == Math.Truncate(d) && !double.IsInfinity(d)
                    ? d.ToString("F2")
                    : d.ToString("G6");

            if (val is float f)
                return f.ToString("G6");

            if (val is DateTime dt)
                return dt.ToString("yyyy-MM-dd");

            if (val is bool b)
                return b ? "True" : "False";

            return val?.ToString() ?? NullLabel;
        }

        private static bool IsNumericColumn(BaseColumn col)
        {
            return col is DataColumn<double>
                || col is DataColumn<float>
                || col is DataColumn<int>
                || col is DataColumn<long>
                || col is DataColumn<decimal>;
        }

        private static string GetDtypeLabel(BaseColumn col)
        {
            if (col is DataColumn<double>) return "float64";
            if (col is DataColumn<float>) return "float32";
            if (col is DataColumn<int>) return "int32";
            if (col is DataColumn<long>) return "int64";
            if (col is DataColumn<bool>) return "bool";
            if (col is DataColumn<DateTime>) return "datetime64";
            if (col is CategoricalColumn) return "category";
            if (col is StringColumn) return "object";
            return col.GetType().Name;
        }

        private static string Truncate(string s, int maxWidth)
        {
            if (s == null) return NullLabel;
            if (s.Length <= maxWidth) return s;
            return s.Substring(0, maxWidth - 1) + "…";
        }
    }
}