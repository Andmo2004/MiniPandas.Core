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
        // ── Umbral para detección automática de categóricas ───────────────────

        /// <summary>
        /// Si el ratio (valores únicos / total filas) de una columna string es menor
        /// que este umbral, se construye como CategoricalColumn en lugar de StringColumn.
        ///
        /// Valor por defecto: 0.5 (50%). Ajusta según tus datos:
        ///   - Datos muy repetitivos (países, estados, sexo): 0.1 es suficiente.
        ///   - Datos semilibres (nombres propios): sube a 0.8 o desactiva con 1.0.
        ///
        /// Para desactivar la detección automática: SchemaInference.CategoricalThreshold = 1.0
        ///
        /// ADVERTENCIA: propiedad estática mutable. No modificar desde múltiples
        /// hilos simultáneamente mientras haya cargas en curso.
        /// </summary>
        public static double CategoricalThreshold { get; set; } = 0.5;

        // ── Cadena de inferrers (orden = prioridad) ───────────────────────────

        // Orden deliberado: más restrictivo → más general.
        //
        //   1. IntInferrer     — enteros puros (int/long sin parte decimal).
        //                        Debe ir ANTES que DoubleInferrer: un int es también
        //                        parseable como double, pero int ocupa menos memoria
        //                        y preserva la especialización de Aggregations para
        //                        Sum con acumulador long (evita overflow).
        //
        //   2. DoubleInferrer  — numérico con decimales o strings numéricos.
        //                        Excluye int/long nativos para no solaparse con IntInferrer.
        //
        //   3. DateTimeInferrer — fechas nativas o strings parseables como fecha.
        //                        Va después de los numéricos: "2024" es ambiguo pero
        //                        se trata como número, no como año-fecha.
        //
        // StringInferrer y CategoricalInferrer son los fallbacks y no van en esta lista.
        private static readonly IReadOnlyList<ITypeInferrer> Inferrers =
            new ITypeInferrer[]
            {
                new IntInferrer(),
                new DoubleInferrer(),
                new DateTimeInferrer(),
                // Añade aquí: new BoolInferrer(), etc.
            };

        // ── API pública ───────────────────────────────────────────────────────

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
                yield return InferColumn(names[colIdx], colIdx, rawRows, rowCount);
        }

        // ── Lógica de inferencia ──────────────────────────────────────────────

        private static BaseColumn InferColumn(
            string name, int colIdx, List<object[]> rows, int rowCount)
        {
            // 1. Intentar tipos numéricos y de fecha primero
            foreach (var inferrer in Inferrers)
            {
                if (inferrer.CanHandle(colIdx, rows))
                    return inferrer.Build(name, colIdx, rows, rowCount);
            }

            // 2. Fallback a string: decidir si Categorical o String
            return BuildStringOrCategorical(name, colIdx, rows, rowCount);
        }

        /// <summary>
        /// Determina si la columna string se beneficia de codificación categórica.
        /// Hace UNA sola pasada para recoger los datos y contar valores únicos.
        /// Si cardinalidad / total &lt; CategoricalThreshold → CategoricalColumn.
        /// </summary>
        private static BaseColumn BuildStringOrCategorical(
            string name, int colIdx, List<object[]> rows, int rowCount)
        {
            if (rowCount == 0 || CategoricalThreshold >= 1.0)
                return BuildStringColumn(name, colIdx, rows, rowCount);

            // Una sola pasada: recogemos datos y contamos únicos simultáneamente
            var unique = new HashSet<string>(StringComparer.Ordinal);
            var data = new string[rowCount];

            for (int r = 0; r < rowCount; r++)
            {
                var str = rows[r][colIdx]?.ToString();   // null si celda nula
                data[r] = str;
                if (str != null) unique.Add(str);
            }

            double cardinalityRatio = (double)unique.Count / rowCount;

            // Alta repetición → Categorical ahorra memoria significativamente
            if (cardinalityRatio < CategoricalThreshold)
                return new CategoricalColumn(name, data);

            // Alta cardinalidad (IDs, nombres propios, texto libre) → StringColumn
            return new StringColumn(name, data);
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


    // ── Contrato ──────────────────────────────────────────────────────────────

    public interface ITypeInferrer
    {
        /// <summary>Devuelve true si TODAS las celdas no nulas encajan en este tipo.</summary>
        bool CanHandle(int colIdx, List<object[]> rows);

        /// <summary>Construye la columna tipada asumiendo que CanHandle fue true.</summary>
        BaseColumn Build(string name, int colIdx, List<object[]> rows, int rowCount);
    }


    // ── Implementaciones ──────────────────────────────────────────────────────

    /// <summary>
    /// Infiere columnas de enteros puros.
    /// Acepta int y long nativos (Excel los devuelve como tal para números sin decimales)
    /// y strings que sean enteros válidos.
    /// Produce DataColumn&lt;int&gt;. Si algún valor supera el rango de int se rechaza
    /// la columna entera — DoubleInferrer la recogerá a continuación.
    /// </summary>
    public sealed class IntInferrer : ITypeInferrer
    {
        public bool CanHandle(int colIdx, List<object[]> rows)
        {
            foreach (var row in rows)
            {
                var raw = row[colIdx];
                if (raw == null) continue;

                // double/float con parte decimal → no es entero puro
                if (raw is double d && d != System.Math.Truncate(d)) return false;
                if (raw is float f && f != System.Math.Truncate(f)) return false;

                bool fits =
                    raw is int ||
                    (raw is long l && l >= int.MinValue && l <= int.MaxValue) ||
                    (raw is double d2 && d2 >= int.MinValue && d2 <= int.MaxValue) ||
                    (raw is float f2 && f2 >= int.MinValue && f2 <= int.MaxValue) ||
                    (raw is string s && int.TryParse(s, out _));

                if (!fits) return false;
            }
            return true;
        }

        public BaseColumn Build(string name, int colIdx, List<object[]> rows, int rowCount)
        {
            var col = new DataColumn<int>(name, rowCount);
            for (int r = 0; r < rowCount; r++)
            {
                var raw = rows[r][colIdx];
                col[r] = raw == null ? (int?)null : ToInt(raw);
            }
            return col;
        }

        private static int ToInt(object raw)
        {
            if (raw is int i) return i;
            if (raw is long l) return (int)l;
            if (raw is double d) return (int)d;
            if (raw is float f) return (int)f;
            if (raw is string s) return int.Parse(s, CultureInfo.InvariantCulture);
            return Convert.ToInt32(raw, CultureInfo.InvariantCulture);
        }
    }

    /// <summary>
    /// Infiere columnas numéricas con decimales.
    /// Excluye valores int y long nativos sin parte decimal: IntInferrer tiene prioridad
    /// sobre ellos. Solo llega aquí si IntInferrer ya descartó la columna.
    /// </summary>
    public sealed class DoubleInferrer : ITypeInferrer
    {
        public bool CanHandle(int colIdx, List<object[]> rows)
        {
            foreach (var row in rows)
            {
                var raw = row[colIdx];
                if (raw == null) continue;

                // int y long nativos sin parte decimal → IntInferrer tiene prioridad
                if (raw is int || raw is long) return false;

                bool fits =
                    raw is double ||
                    raw is float ||
                    raw is decimal ||
                    (raw is string s && double.TryParse(
                        s, NumberStyles.Any, CultureInfo.InvariantCulture, out _));

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