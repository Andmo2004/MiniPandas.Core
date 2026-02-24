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

            if (func == AggFunc.Count) return CountNonNull(column, indices);
            if (func == AggFunc.First) return First(column, indices);
            if (func == AggFunc.Last) return Last(column, indices);
            if (func == AggFunc.NUnique) return CountUnique(column, indices);

            if (column is DataColumn<double> dcDouble)
                return AggregateDouble(dcDouble, indices, func);

            if (column is DataColumn<int> dcInt)
                return AggregateInt(dcInt, indices, func);

            throw new InvalidOperationException(
                $"AggFunc.{func} is not supported for column '{column.Name}' " +
                $"of type {column.GetType().Name}.");
        }

        // ── double ────────────────────────────────────────────────────────────

        private static object AggregateDouble(DataColumn<double> col, int[] indices, AggFunc func)
        {
            switch (func)
            {
                case AggFunc.Sum: return SumDouble(col, indices);
                case AggFunc.Mean: return MeanDouble(col, indices);
                case AggFunc.Min: return MinDouble(col, indices);
                case AggFunc.Max: return MaxDouble(col, indices);
                case AggFunc.Std: return StdDouble(col, indices);
                case AggFunc.Var: return VarDouble(col, indices);
                case AggFunc.Prod: return ProdDouble(col, indices);
                case AggFunc.Median: return MedianDouble(col, indices);
                default:
                    throw new ArgumentOutOfRangeException(nameof(func), func, null);
            }
        }

        private static object SumDouble(DataColumn<double> col, int[] indices)
        {
            double acc = 0;
            int count = 0;
            foreach (int i in indices)
            {
                if (col.IsNull(i)) continue;
                acc += col.GetRawValue(i);
                count++;
            }
            return count == 0 ? (object)null : acc;
        }

        private static object MeanDouble(DataColumn<double> col, int[] indices)
        {
            double acc = 0;
            int count = 0;
            foreach (int i in indices)
            {
                if (col.IsNull(i)) continue;
                acc += col.GetRawValue(i);
                count++;
            }
            return count == 0 ? (object)null : acc / count;
        }

        private static object MinDouble(DataColumn<double> col, int[] indices)
        {
            double min = double.MaxValue;
            int count = 0;
            foreach (int i in indices)
            {
                if (col.IsNull(i)) continue;
                double v = col.GetRawValue(i);
                if (v < min) min = v;
                count++;
            }
            return count == 0 ? (object)null : min;
        }

        private static object MaxDouble(DataColumn<double> col, int[] indices)
        {
            double max = double.MinValue;
            int count = 0;
            foreach (int i in indices)
            {
                if (col.IsNull(i)) continue;
                double v = col.GetRawValue(i);
                if (v > max) max = v;
                count++;
            }
            return count == 0 ? (object)null : max;
        }

        private static object StdDouble(DataColumn<double> col, int[] indices)
        {
            double acc = 0;
            int count = 0;
            foreach (int i in indices)
            {
                if (col.IsNull(i)) continue;
                acc += col.GetRawValue(i);
                count++;
            }
            if (count < 2) return null;

            double mean = acc / count;
            double sumSq = 0;
            foreach (int i in indices)
            {
                if (col.IsNull(i)) continue;
                double diff = col.GetRawValue(i) - mean;
                sumSq += diff * diff;
            }
            return System.Math.Sqrt(sumSq / (count - 1));
        }

        private static object VarDouble(DataColumn<double> col, int[] indices)
        {
            double acc = 0;
            int count = 0;
            foreach (int i in indices)
            {
                if (col.IsNull(i)) continue;
                acc += col.GetRawValue(i);
                count++;
            }
            if (count < 2) return null;

            double mean = acc / count;
            double sumSq = 0;
            foreach (int i in indices)
            {
                if (col.IsNull(i)) continue;
                double diff = col.GetRawValue(i) - mean;
                sumSq += diff * diff;
            }
            return sumSq / (count - 1);
        }

        private static object ProdDouble(DataColumn<double> col, int[] indices)
        {
            double acc = 1.0;
            int count = 0;
            foreach (int i in indices)
            {
                if (col.IsNull(i)) continue;
                acc *= col.GetRawValue(i);
                count++;
            }
            return count == 0 ? (object)null : acc;
        }

        private static object MedianDouble(DataColumn<double> col, int[] indices)
        {
            var values = new List<double>(indices.Length);
            foreach (int i in indices)
            {
                if (!col.IsNull(i))
                    values.Add(col.GetRawValue(i));
            }
            if (values.Count == 0) return null;

            values.Sort();
            int mid = values.Count / 2;
            return values.Count % 2 != 0
                ? values[mid]
                : (values[mid - 1] + values[mid]) / 2.0;
        }

        // ── int ───────────────────────────────────────────────────────────────

        private static object AggregateInt(DataColumn<int> col, int[] indices, AggFunc func)
        {
            switch (func)
            {
                case AggFunc.Sum:
                    {
                        long acc = 0;
                        int count = 0;
                        foreach (int i in indices)
                        {
                            if (col.IsNull(i)) continue;
                            acc += col.GetRawValue(i);
                            count++;
                        }
                        return count == 0 ? (object)null : (double)acc;
                    }
                case AggFunc.Mean:
                    {
                        long acc = 0;
                        int count = 0;
                        foreach (int i in indices)
                        {
                            if (col.IsNull(i)) continue;
                            acc += col.GetRawValue(i);
                            count++;
                        }
                        return count == 0 ? (object)null : (double)acc / count;
                    }
                case AggFunc.Min:
                    {
                        int min = int.MaxValue;
                        int count = 0;
                        foreach (int i in indices)
                        {
                            if (col.IsNull(i)) continue;
                            int v = col.GetRawValue(i);
                            if (v < min) min = v;
                            count++;
                        }
                        return count == 0 ? (object)null : (double)min;
                    }
                case AggFunc.Max:
                    {
                        int max = int.MinValue;
                        int count = 0;
                        foreach (int i in indices)
                        {
                            if (col.IsNull(i)) continue;
                            int v = col.GetRawValue(i);
                            if (v > max) max = v;
                            count++;
                        }
                        return count == 0 ? (object)null : (double)max;
                    }
                case AggFunc.Std:
                    {
                        double acc = 0;
                        int count = 0;
                        foreach (int i in indices)
                        {
                            if (col.IsNull(i)) continue;
                            acc += col.GetRawValue(i);
                            count++;
                        }
                        if (count < 2) return null;
                        double mean = acc / count;
                        double sumSq = 0;
                        foreach (int i in indices)
                        {
                            if (col.IsNull(i)) continue;
                            double diff = col.GetRawValue(i) - mean;
                            sumSq += diff * diff;
                        }
                        return System.Math.Sqrt(sumSq / (count - 1));
                    }
                case AggFunc.Var:
                    {
                        double acc = 0;
                        int count = 0;
                        foreach (int i in indices)
                        {
                            if (col.IsNull(i)) continue;
                            acc += col.GetRawValue(i);
                            count++;
                        }
                        if (count < 2) return null;
                        double mean = acc / count;
                        double sumSq = 0;
                        foreach (int i in indices)
                        {
                            if (col.IsNull(i)) continue;
                            double diff = col.GetRawValue(i) - mean;
                            sumSq += diff * diff;
                        }
                        return sumSq / (count - 1);
                    }
                case AggFunc.Prod:
                    {
                        // Devolvemos double para evitar overflow
                        double acc = 1.0;
                        int count = 0;
                        foreach (int i in indices)
                        {
                            if (col.IsNull(i)) continue;
                            acc *= col.GetRawValue(i);
                            count++;
                        }
                        return count == 0 ? (object)null : acc;
                    }
                case AggFunc.Median:
                    {
                        var values = new List<int>(indices.Length);
                        foreach (int i in indices)
                        {
                            if (!col.IsNull(i))
                                values.Add(col.GetRawValue(i));
                        }
                        if (values.Count == 0) return null;
                        values.Sort();
                        int mid = values.Count / 2;
                        return values.Count % 2 != 0
                            ? (double)values[mid]
                            : (values[mid - 1] + values[mid]) / 2.0;
                    }
                default:
                    throw new ArgumentOutOfRangeException(nameof(func), func, null);
            }
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