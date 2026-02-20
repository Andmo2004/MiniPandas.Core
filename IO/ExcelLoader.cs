using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using ExcelDataReader;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.IO
{
    public static class ExcelLoader
    {
        public static DataFrame LoadExcel(string path, bool hasHeader = true)
        {
            if (!File.Exists(path))
                throw new FileNotFoundException("Excel file not found.", path);

            using (var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var reader = ExcelReaderFactory.CreateReader(stream))
            {
                // ── 1. Leer nombres de columna ───────────────────────────────
                if (!reader.Read())
                    throw new InvalidDataException("The Excel file is empty.");

                int colCount = reader.FieldCount;
                var names = new string[colCount];

                for (int i = 0; i < colCount; i++)
                {
                    names[i] = hasHeader
                        ? (reader.GetValue(i)?.ToString()?.Trim() ?? $"Column{i}")
                        : $"Column{i}";
                }

                // Si no hay encabezado, la primera fila son datos: volvemos al inicio
                if (!hasHeader)
                {
                    reader.Reset();
                    reader.Read();
                }

                // ── 2. Leer todas las filas en memoria ───────────────────────
                // Usamos object[][] para no pre-comprometernos con tipos ni tamaño.
                // reader.RowCount es poco fiable en .xls y en sheets con celdas fantasma.
                var rawRows = new List<object[]>(capacity: 256);

                while (reader.Read())
                {
                    object[] row = new object[colCount];
                    bool isEmpty = true;

                    for (int i = 0; i < colCount; i++)
                    {
                        var val = reader.GetValue(i);
                        // Normalizamos DBNull a null desde el origen
                        row[i] = (val == DBNull.Value) ? null : val;
                        if (isEmpty && row[i] != null)
                            isEmpty = false;
                    }

                    if (!isEmpty) // Descartamos filas completamente vacías
                        rawRows.Add(row);
                }

                int rowCount = rawRows.Count;

                // ── 3. Inferir tipos por columna ─────────────────────────────
                var df = new DataFrame(rowCount);

                for (int colIdx = 0; colIdx < colCount; colIdx++)
                {
                    BaseColumn column = InferAndBuildColumn(
                        names[colIdx], colIdx, rawRows, rowCount);

                    df.AddColumn(column);
                }

                return df;
            }
        }

        // ── Inferencia de tipo: double > DateTime > string ───────────────────
        // Orden deliberado: intentamos el tipo más restrictivo primero.
        // Si una sola celda no encaja (y no es nula), bajamos al siguiente.
        private static BaseColumn InferAndBuildColumn(
            string name, int colIdx, List<object[]> rows, int rowCount)
        {
            bool canBeDouble   = true;
            bool canBeDateTime = true;

            for (int r = 0; r < rowCount && (canBeDouble || canBeDateTime); r++)
            {
                var raw = rows[r][colIdx];
                if (raw == null) continue; // nulo no descarta ningún tipo

                // ExcelDataReader ya devuelve double/DateTime nativos para celdas numéricas y de fecha
                if (canBeDouble)
                {
                    canBeDouble = raw is double || raw is int || raw is long || raw is float || raw is decimal
                    || (raw is string s && double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out _));
                }

                if (canBeDateTime)
                {
                    canBeDateTime = raw is DateTime
                        || (raw is string ds && DateTime.TryParse(
                                ds, CultureInfo.InvariantCulture, DateTimeStyles.None, out _));
                }
            }

            if (canBeDouble)   return BuildColumn<double>(name, colIdx, rows, rowCount, ToDouble);
            if (canBeDateTime) return BuildColumn<DateTime>(name, colIdx, rows, rowCount, ToDateTime);
            return BuildStringColumn(name, colIdx, rows, rowCount);
        }

        private static DataColumn<T> BuildColumn<T>(
            string name, int colIdx,
            List<object[]> rows, int rowCount,
            Func<object, T> convert) where T : struct
        {
            var col = new DataColumn<T>(name, rowCount);
            for (int r = 0; r < rowCount; r++)
            {
                var raw = rows[r][colIdx];
                if (raw == null)
                    col[r] = null;          // nulo semántico
                else
                    col[r] = convert(raw);
            }
            return col;
        }

        private static StringColumn BuildStringColumn(
            string name, int colIdx, List<object[]> rows, int rowCount)
        {
            var col = new StringColumn(name, rowCount);
            for (int r = 0; r < rowCount; r++)
                col[r] = rows[r][colIdx]?.ToString(); // null permanece null
            return col;
        }

        // ── Conversores robustos ─────────────────────────────────────────────
        private static double ToDouble(object raw)
        {
            if (raw is double d) return d;
            if (raw is int i) return i;
            if (raw is long l) return l;
            if (raw is float f) return f;
            if (raw is decimal m) return (double)m;
            if (raw is string s) return double.Parse(s, CultureInfo.InvariantCulture);
            return Convert.ToDouble(raw, CultureInfo.InvariantCulture);
        }

        private static DateTime ToDateTime(object raw)
        {
            if (raw is DateTime dt) return dt;
            if (raw is string s) return DateTime.Parse(s, CultureInfo.InvariantCulture);
            return Convert.ToDateTime(raw, CultureInfo.InvariantCulture);
        }
    }
}