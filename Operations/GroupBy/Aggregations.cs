using System;
using System.Collections.Generic;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.Operations.GroupBy
{
    /// <summary>
    /// Calcula agregaciones sobre un subconjunto de filas de una columna.
    ///
    /// DISEÑO:
    ///   Recibe la columna completa + una lista de índices (el grupo).
    ///   No materializa subcolumnas: opera directamente sobre los índices.
    ///   Devuelve object para poder construir columnas heterogéneas en el resultado.
    ///
    ///   El boxing es inevitable aquí porque el resultado de Agg() es un DataFrame
    ///   donde cada celda puede ser de tipo distinto. Se localiza en este único punto.
    ///
    /// ESTRATEGIA ANTI-DUPLICACIÓN:
    ///   CollectDoubles() convierte cualquier columna numérica soportada a double[].
    ///   Toda la matemática vive en helpers que operan sobre double[].
    ///   Añadir un nuevo tipo numérico (float, long, decimal) solo requiere tocar
    ///   CollectDoubles() — la lógica de agregación no cambia.
    /// </summary>
    internal static class Aggregations
    {
        /// <summary>
        /// Aplica una función de agregación a un subconjunto de filas de una columna.
        /// Devuelve null si el grupo está vacío o todos los valores son nulos.
        /// </summary>
        public static object Aggregate(BaseColumn column, int[] indices, AggFunc func)
        {
            if (column == null) throw new ArgumentNullException(nameof(column));
            if (indices == null) throw new ArgumentNullException(nameof(indices));

            // Funciones genéricas: no necesitan tipo numérico
            if (func == AggFunc.Count) return CountNonNull(column, indices);
            if (func == AggFunc.First) return First(column, indices);
            if (func == AggFunc.Last) return Last(column, indices);
            if (func == AggFunc.NUnique) return CountUnique(column, indices);

            // Funciones numéricas: dispatch de tipo en un único punto
            double[] values = CollectDoubles(column, indices);
            return AggregateValues(values, func, column.Name);
        }

        // ── Recolección de valores — único punto de dispatch por tipo ─────────

        /// <summary>
        /// Extrae los valores no nulos de una columna numérica soportada como double[].
        /// Centraliza el dispatch por tipo: añadir DataColumn&lt;float&gt; solo requiere
        /// un nuevo bloque 'else if' aquí.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Si el tipo de columna no es numérico soportado.
        /// </exception>
        private static double[] CollectDoubles(BaseColumn column, int[] indices)
        {
            var values = new List<double>(indices.Length);

            if (column is DataColumn<double> dcDouble)
            {
                foreach (int i in indices)
                    if (!dcDouble.IsNull(i)) values.Add(dcDouble.GetRawValue(i));
            }
            else if (column is DataColumn<int> dcInt)
            {
                // int cabe exactamente en double hasta 2^53: no hay pérdida de precisión
                // para valores dentro del rango normal de un DataColumn<int>.
                foreach (int i in indices)
                    if (!dcInt.IsNull(i)) values.Add(dcInt.GetRawValue(i));
            }
            else
            {
                throw new InvalidOperationException(
                    $"Column '{column.Name}' of type {column.GetType().Name} " +
                    $"is not a supported numeric column for aggregation.");
            }

            return values.ToArray();
        }

        // ── Dispatcher numérico — opera sobre double[] ────────────────────────

        private static object AggregateValues(double[] values, AggFunc func, string columnName)
        {
            switch (func)
            {
                case AggFunc.Sum: return SumValues(values);
                case AggFunc.Mean: return MeanValues(values);
                case AggFunc.Min: return MinValues(values);
                case AggFunc.Max: return MaxValues(values);
                case AggFunc.Std: return StdValues(values);
                case AggFunc.Var: return VarValues(values);
                case AggFunc.Prod: return ProdValues(values);
                case AggFunc.Median: return MedianValues(values);
                default:
                    throw new ArgumentOutOfRangeException(nameof(func), func,
                        $"AggFunc.{func} reached numeric dispatcher unexpectedly for column '{columnName}'.");
            }
        }

        // ── Helpers matemáticos — cada función existe una sola vez ────────────

        private static object SumValues(double[] values)
        {
            if (values.Length == 0) return null;
            double acc = 0;
            foreach (double v in values) acc += v;
            return acc;
        }

        private static object MeanValues(double[] values)
        {
            if (values.Length == 0) return null;
            double acc = 0;
            foreach (double v in values) acc += v;
            return acc / values.Length;
        }

        private static object MinValues(double[] values)
        {
            if (values.Length == 0) return null;
            double min = double.MaxValue;
            foreach (double v in values)
                if (v < min) min = v;
            return min;
        }

        private static object MaxValues(double[] values)
        {
            if (values.Length == 0) return null;
            double max = double.MinValue;
            foreach (double v in values)
                if (v > max) max = v;
            return max;
        }

        private static object ProdValues(double[] values)
        {
            if (values.Length == 0) return null;
            double acc = 1.0;
            foreach (double v in values) acc *= v;
            return acc;
        }

        private static object MedianValues(double[] values)
        {
            if (values.Length == 0) return null;

            // Trabajamos sobre una copia para no alterar el array original
            var sorted = (double[])values.Clone();
            Array.Sort(sorted);
            int mid = sorted.Length / 2;
            return sorted.Length % 2 != 0
                ? sorted[mid]
                : (sorted[mid - 1] + sorted[mid]) / 2.0;
        }

        /// <summary>
        /// Varianza muestral (n-1). Usada tanto por Var como por Std.
        /// Devuelve null si hay menos de 2 valores (varianza indefinida).
        /// </summary>
        private static double? ComputeVariance(double[] values)
        {
            if (values.Length < 2) return null;

            double acc = 0;
            foreach (double v in values) acc += v;
            double mean = acc / values.Length;

            double sumSq = 0;
            foreach (double v in values)
            {
                double diff = v - mean;
                sumSq += diff * diff;
            }
            return sumSq / (values.Length - 1);
        }

        private static object VarValues(double[] values)
        {
            double? variance = ComputeVariance(values);
            return variance.HasValue ? (object)variance.Value : null;
        }

        private static object StdValues(double[] values)
        {
            double? variance = ComputeVariance(values);
            return variance.HasValue ? (object)System.Math.Sqrt(variance.Value) : null;
        }

        // ── Genéricas (cualquier tipo de columna) ─────────────────────────────

        private static object CountNonNull(BaseColumn col, int[] indices)
        {
            int count = 0;
            foreach (int i in indices)
                if (!col.IsNull(i)) count++;
            return count;
        }

        private static object CountUnique(BaseColumn col, int[] indices)
        {
            // Fast path para CategoricalColumn: los valores únicos se cuentan comparando
            // códigos enteros en lugar de strings. Evita descodificar y boxear cada celda,
            // que es exactamente lo que CategoricalColumn está diseñada para evitar.
            // El código -1 (nulo) ya se descarta con IsNull antes de llegar aquí.
            if (col is CategoricalColumn cc)
            {
                var uniqueCodes = new HashSet<int>();
                foreach (int i in indices)
                    if (!cc.IsNull(i)) uniqueCodes.Add(cc.GetCode(i));
                return uniqueCodes.Count;
            }

            // Ruta general para StringColumn, DataColumn<T> y cualquier tipo futuro.
            // GetBoxed es inevitable aquí: no conocemos el tipo concreto.
            var uniqueSet = new HashSet<object>();
            foreach (int i in indices)
            {
                if (!col.IsNull(i))
                    uniqueSet.Add(col.GetBoxed(i));
            }
            return uniqueSet.Count;
        }

        private static object First(BaseColumn col, int[] indices)
        {
            foreach (int i in indices)
                if (!col.IsNull(i)) return col.GetBoxed(i);
            return null;
        }

        private static object Last(BaseColumn col, int[] indices)
        {
            object last = null;
            foreach (int i in indices)
                if (!col.IsNull(i)) last = col.GetBoxed(i);
            return last;
        }
    }
}