using System;
using System.Collections.Generic;
using System.Globalization;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.IO
{
    /// <summary>
    /// Infiere el tipo de cada columna a partir de datos crudos (object[][])
    /// y construye las columnas tipadas del DataFrame.
    /// Reutilizable por cualquier loader (Excel, CSV, JSON, etc.).
    /// </summary>
    public static class SchemaInference
    {
        // Orden de preferencia: más restrictivo → más general
        private static readonly IReadOnlyList<ITypeInferrer> Inferrers =
            new ITypeInferrer[]
            {
                new DoubleInferrer(),
                new DateTimeInferrer(),
                // Aquí puedes añadir BoolInferrer, IntInferrer, etc.
                // StringInferrer es el fallback y no participa en la cadena.
            };

        /// <summary>
        /// Recorre las columnas de rawRows, infiere su tipo y devuelve las columnas construidas.
        /// </summary>
        public static IEnumerable<BaseColumn> InferColumns(
            string[] names,
            List<object[]> rawRows)
        {
            if (names == null) throw new ArgumentNullException(nameof(names));
            if (rawRows == null) throw new ArgumentNullException(nameof(rawRows));

            int rowCount = rawRows.Count;

            for (int colIdx = 0; colIdx < names.Length; colIdx++)
            {
                yield return InferColumn(names[colIdx], colIdx, rawRows, rowCount);
            }
        }

        private static BaseColumn InferColumn(
            string name, int colIdx, List<object[]> rows, int rowCount)
        {
            foreach (var inferrer in Inferrers)
            {
                if (inferrer.CanHandle(colIdx, rows))
                    return inferrer.Build(name, colIdx, rows, rowCount);
            }

            // Fallback garantizado: siempre podemos representar cualquier cosa como string
            return BuildStringColumn(name, colIdx, rows, rowCount);
        }

        private static StringColumn BuildStringColumn(
            string name, int colIdx, List<object[]> rows, int rowCount)
        {
            var col = new StringColumn(name, rowCount);
            for (int r = 0; r < rowCount; r++)
                col[r] = rows[r][colIdx]?.ToString();
            return col;
        }
    }


    // ── Contrato ─────────────────────────────────────────────────────────────

    public interface ITypeInferrer
    {
        /// <summary>Devuelve true si TODAS las celdas no nulas encajan en este tipo.</summary>
        bool CanHandle(int colIdx, List<object[]> rows);

        /// <summary>Construye la columna tipada asumiendo que CanHandle fue true.</summary>
        BaseColumn Build(string name, int colIdx, List<object[]> rows, int rowCount);
    }


    // ── Implementaciones ─────────────────────────────────────────────────────

    public sealed class DoubleInferrer : ITypeInferrer
    {
        public bool CanHandle(int colIdx, List<object[]> rows)
        {
            foreach (var row in rows)
            {
                var raw = row[colIdx];
                if (raw == null) continue;

                bool fits = raw is double || raw is int || raw is long || raw is float || raw is decimal
                    || (raw is string s && double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out _));

                if (!fits) return false;
            }
            return true;
        }

        public BaseColumn Build(string name, int colIdx, List<object[]> rows, int rowCount)
        {
            var col = new DataColumn<double>(name, rowCount);
            for (int r = 0; r < rowCount; r++)
            {
                var raw = rows[r][colIdx];
                col[r] = raw == null ? (double?)null : ToDouble(raw);
            }
            return col;
        }

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
    }

    public sealed class DateTimeInferrer : ITypeInferrer
    {
        public bool CanHandle(int colIdx, List<object[]> rows)
        {
            foreach (var row in rows)
            {
                var raw = row[colIdx];
                if (raw == null) continue;

                bool fits = raw is DateTime
                    || (raw is string s && DateTime.TryParse(
                            s, CultureInfo.InvariantCulture, DateTimeStyles.None, out _));

                if (!fits) return false;
            }
            return true;
        }

        public BaseColumn Build(string name, int colIdx, List<object[]> rows, int rowCount)
        {
            var col = new DataColumn<DateTime>(name, rowCount);
            for (int r = 0; r < rowCount; r++)
            {
                var raw = rows[r][colIdx];
                col[r] = raw == null ? (DateTime?)null : ToDateTime(raw);
            }
            return col;
        }

        private static DateTime ToDateTime(object raw)
        {
            if (raw is DateTime dt) return dt;
            if (raw is string s) return DateTime.Parse(s, CultureInfo.InvariantCulture);
            return Convert.ToDateTime(raw, CultureInfo.InvariantCulture);
        }
    }
}