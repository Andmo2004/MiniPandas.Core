using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core
{
    public partial class DataFrame
    {
        // ── Constantes de configuración ───────────────────────────────────────

        private const int DefaultMaxRows = 10;   // filas visibles antes de truncar
        private const int DefaultMaxColWidth = 12;  // ancho máximo de cada celda
        private const int IndexColWidth = 6;    // ancho de la columna de índice
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
        public void Print(int maxRows = DefaultMaxRows, int maxColWidth = DefaultMaxColWidth)
        {
            Console.WriteLine(ToString(maxRows, maxColWidth));
        }

        /// <summary>
        /// Versión que escribe en cualquier writer (útil para tests y logging).
        /// </summary>
        public void Print(System.IO.TextWriter writer, int maxRows = DefaultMaxRows, int maxColWidth = DefaultMaxColWidth)
        {
            writer.WriteLine(ToString(maxRows, maxColWidth));
        }

        /// <summary>
        /// Devuelve la representación tabular como string (sin imprimirla).
        /// Equivalente al __repr__ de pandas.
        /// </summary>
        public string ToString(int maxRows = DefaultMaxRows, int maxColWidth = DefaultMaxColWidth)
        {
            if (RowCount == 0 || ColumnCount == 0)
                return BuildEmptyMessage();

            // ── 1. Determinar qué filas mostrar ───────────────────────────────
            int[] rowIndices = GetRowIndices(maxRows, out bool truncated);

            // ── 2. Calcular anchos de columna ─────────────────────────────────
            int[] colWidths = ComputeColumnWidths(rowIndices, maxColWidth);
            string[] colNames = ColumnNames.ToArray();

            // ── 3. Construir la salida ─────────────────────────────────────────
            var sb = new StringBuilder();

            // Cabecera
            AppendHeader(sb, colNames, colWidths);

            // Separador
            AppendSeparator(sb, colWidths);

            // Filas de datos
            int half = rowIndices.Length;
            if (truncated)
            {
                int headCount = maxRows / 2;
                int tailCount = maxRows - headCount;

                // Filas de cabeza
                for (int i = 0; i < headCount; i++)
                    AppendRow(sb, rowIndices[i], colNames, colWidths);

                // Línea de elipsis
                AppendEllipsisRow(sb, colWidths);

                // Filas de cola
                for (int i = headCount; i < headCount + tailCount; i++)
                    AppendRow(sb, rowIndices[i], colNames, colWidths);
            }
            else
            {
                foreach (int idx in rowIndices)
                    AppendRow(sb, idx, colNames, colWidths);
            }

            // Resumen
            sb.AppendLine();
            sb.Append($"[{RowCount} rows × {ColumnCount} columns]");

            // Tipos de columnas (una línea debajo, igual que pandas con dtypes)
            AppendDtypes(sb, colNames);

            return sb.ToString();
        }

        // ── Helpers privados ──────────────────────────────────────────────────

        private string BuildEmptyMessage()
        {
            if (ColumnCount == 0)
                return $"Empty DataFrame\nColumns: []\n[0 rows × 0 columns]";

            var cols = string.Join(", ", ColumnNames);
            return $"Empty DataFrame\nColumns: [{cols}]\n[0 rows × {ColumnCount} columns]";
        }

        /// <summary>
        /// Devuelve los índices de fila a mostrar.
        /// Si el total supera maxRows, devuelve cabeza + cola (igual que pandas).
        /// </summary>
        private int[] GetRowIndices(int maxRows, out bool truncated)
        {
            if (maxRows <= 0 || RowCount <= maxRows)
            {
                truncated = false;
                return Enumerable.Range(0, RowCount).ToArray();
            }

            truncated = true;
            int head = maxRows / 2;
            int tail = maxRows - head;

            var indices = new int[head + tail];
            for (int i = 0; i < head; i++)
                indices[i] = i;
            for (int i = 0; i < tail; i++)
                indices[head + i] = RowCount - tail + i;

            return indices;
        }

        /// <summary>
        /// Calcula el ancho óptimo de cada columna:
        /// max(nombre_columna, valor_más_largo) acotado a maxColWidth.
        /// </summary>
        private int[] ComputeColumnWidths(int[] rowIndices, int maxColWidth)
        {
            string[] colNames = ColumnNames.ToArray();
            int[] widths = new int[colNames.Length];

            for (int c = 0; c < colNames.Length; c++)
            {
                int w = Math.Min(colNames[c].Length, maxColWidth);
                BaseColumn col = this[colNames[c]];

                foreach (int r in rowIndices)
                {
                    string cell = FormatCell(col, r);
                    w = Math.Max(w, Math.Min(cell.Length, maxColWidth));
                }

                widths[c] = Math.Max(w, Ellipsis.Length); // al menos 3 ("...")
            }

            return widths;
        }

        /// <summary>
        /// Escribe la fila de cabecera con los nombres de columna.
        /// </summary>
        private void AppendHeader(StringBuilder sb, string[] colNames, int[] colWidths)
        {
            // Espacio para el índice
            sb.Append(new string(' ', IndexColWidth + 1));

            for (int c = 0; c < colNames.Length; c++)
            {
                string name = Truncate(colNames[c], colWidths[c]);

                // Las columnas numéricas se alinean a la derecha; las de texto a la izquierda
                if (IsNumericColumn(this[colNames[c]]))
                    sb.Append(name.PadLeft(colWidths[c]));
                else
                    sb.Append(name.PadRight(colWidths[c]));

                if (c < colNames.Length - 1) sb.Append("  ");
            }
            sb.AppendLine();
        }

        /// <summary>
        /// Escribe una línea separadora de caracteres "─".
        /// </summary>
        private void AppendSeparator(StringBuilder sb, int[] colWidths)
        {
            int totalWidth = IndexColWidth + 1
                + colWidths.Sum()
                + (colWidths.Length - 1) * 2;  // separador de 2 espacios entre columnas

            sb.AppendLine(new string('─', totalWidth));
        }

        /// <summary>
        /// Escribe una fila de datos con su índice.
        /// </summary>
        private void AppendRow(StringBuilder sb, int rowIdx, string[] colNames, int[] colWidths)
        {
            // Índice (alineado a la derecha)
            sb.Append(rowIdx.ToString().PadLeft(IndexColWidth));
            sb.Append(' ');

            for (int c = 0; c < colNames.Length; c++)
            {
                BaseColumn col = this[colNames[c]];
                string cell = Truncate(FormatCell(col, rowIdx), colWidths[c]);

                if (IsNumericColumn(col))
                    sb.Append(cell.PadLeft(colWidths[c]));
                else
                    sb.Append(cell.PadRight(colWidths[c]));

                if (c < colNames.Length - 1) sb.Append("  ");
            }
            sb.AppendLine();
        }

        /// <summary>
        /// Escribe la fila de puntos suspensivos cuando hay truncado.
        /// </summary>
        private void AppendEllipsisRow(StringBuilder sb, int[] colWidths)
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

        /// <summary>
        /// Escribe los dtypes de cada columna en una línea resumen.
        /// </summary>
        private void AppendDtypes(StringBuilder sb, string[] colNames)
        {
            sb.AppendLine();
            sb.AppendLine("dtypes:");
            foreach (var name in colNames)
            {
                string dtype = GetDtypeLabel(this[name]);
                sb.AppendLine($"  {name}: {dtype}");
            }
        }

        // ── Formateo de celdas ────────────────────────────────────────────────

        private static string FormatCell(BaseColumn col, int row)
        {
            if (col.IsNull(row)) return NullLabel;

            object val = col.GetBoxed(row);

            if (val is double d)
            {
                // Misma lógica que pandas: si es entero, no muestra decimales
                return d == Math.Truncate(d) && !double.IsInfinity(d)
                    ? d.ToString("F2")   // pandas siempre muestra 2 decimales para doubles
                    : d.ToString("G6");
            }

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