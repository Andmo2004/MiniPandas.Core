// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.Operations.Math
{
    /// <summary>
    /// Operaciones aritméticas vectorizadas sobre columnas numéricas.
    ///
    /// DISEÑO:
    ///   Todas las operaciones son estáticas y devuelven una nueva columna (inmutable).
    ///   Los nulls se propagan: si cualquiera de los operandos es null, el resultado es null.
    ///   Implementadas con loops explícitos — LINQ tiene overhead no despreciable en hot-paths.
    ///
    /// TIPOS SOPORTADOS:
    ///   DataColumn&lt;double&gt; es el tipo numérico principal (como float64 en pandas).
    ///   DataColumn&lt;int&gt; se soporta para operaciones enteras.
    ///   Las operaciones mixtas int+double no existen: convierte primero con AsDouble().
    ///
    /// DIVIDE SOBRE INT:
    ///   Devuelve DataColumn&lt;double&gt;, no DataColumn&lt;int&gt;.
    ///   Razón: la división entera (7/2 = 3) pierde información silenciosamente.
    ///   Consistente con Python (/) y con pandas (int64 / int64 → float64).
    ///   Para división entera explícita convierte primero con AsDouble() y usa
    ///   Math.Floor, o implementa un DivideInteger separado si lo necesitas.
    /// </summary>
    public static class VectorOps
    {
        // ── double: columna OP columna ────────────────────────────────────────

        public static DataColumn<double> Add(DataColumn<double> a, DataColumn<double> b)
        {
            ValidateSameLength(a, b);
            var (data, nulls) = Allocate(a.Length);
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i) || b.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = a.GetRawValue(i) + b.GetRawValue(i);
            }
            return new DataColumn<double>($"{a.Name}+{b.Name}", data, nulls);
        }

        public static DataColumn<double> Subtract(DataColumn<double> a, DataColumn<double> b)
        {
            ValidateSameLength(a, b);
            var (data, nulls) = Allocate(a.Length);
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i) || b.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = a.GetRawValue(i) - b.GetRawValue(i);
            }
            return new DataColumn<double>($"{a.Name}-{b.Name}", data, nulls);
        }

        public static DataColumn<double> Multiply(DataColumn<double> a, DataColumn<double> b)
        {
            ValidateSameLength(a, b);
            var (data, nulls) = Allocate(a.Length);
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i) || b.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = a.GetRawValue(i) * b.GetRawValue(i);
            }
            return new DataColumn<double>($"{a.Name}*{b.Name}", data, nulls);
        }

        public static DataColumn<double> Divide(DataColumn<double> a, DataColumn<double> b)
        {
            ValidateSameLength(a, b);
            var (data, nulls) = Allocate(a.Length);
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i) || b.IsNull(i)) { nulls[i] = true; continue; }
                double divisor = b.GetRawValue(i);
                if (divisor == 0.0) { nulls[i] = true; continue; }   // div/0 → null, no excepción
                data[i] = a.GetRawValue(i) / divisor;
            }
            return new DataColumn<double>($"{a.Name}/{b.Name}", data, nulls);
        }

        // ── double: columna OP escalar ────────────────────────────────────────

        public static DataColumn<double> Add(DataColumn<double> col, double scalar)
        {
            var (data, nulls) = Allocate(col.Length);
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = col.GetRawValue(i) + scalar;
            }
            return new DataColumn<double>(col.Name, data, nulls);
        }

        public static DataColumn<double> Subtract(DataColumn<double> col, double scalar)
        {
            var (data, nulls) = Allocate(col.Length);
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = col.GetRawValue(i) - scalar;
            }
            return new DataColumn<double>(col.Name, data, nulls);
        }

        public static DataColumn<double> Multiply(DataColumn<double> col, double scalar)
        {
            var (data, nulls) = Allocate(col.Length);
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = col.GetRawValue(i) * scalar;
            }
            return new DataColumn<double>(col.Name, data, nulls);
        }

        public static DataColumn<double> Divide(DataColumn<double> col, double scalar)
        {
            if (scalar == 0.0) throw new DivideByZeroException(
                "Scalar divisor cannot be zero. Use column/column Divide if you need null propagation.");
            var (data, nulls) = Allocate(col.Length);
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = col.GetRawValue(i) / scalar;
            }
            return new DataColumn<double>(col.Name, data, nulls);
        }

        // ── int: columna OP columna ───────────────────────────────────────────

        public static DataColumn<int> Add(DataColumn<int> a, DataColumn<int> b)
        {
            ValidateSameLength(a, b);
            var (data, nulls) = AllocateInt(a.Length);
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i) || b.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = a.GetRawValue(i) + b.GetRawValue(i);
            }
            return new DataColumn<int>($"{a.Name}+{b.Name}", data, nulls);
        }

        public static DataColumn<int> Subtract(DataColumn<int> a, DataColumn<int> b)
        {
            ValidateSameLength(a, b);
            var (data, nulls) = AllocateInt(a.Length);
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i) || b.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = a.GetRawValue(i) - b.GetRawValue(i);
            }
            return new DataColumn<int>($"{a.Name}-{b.Name}", data, nulls);
        }

        public static DataColumn<int> Multiply(DataColumn<int> a, DataColumn<int> b)
        {
            ValidateSameLength(a, b);
            var (data, nulls) = AllocateInt(a.Length);
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i) || b.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = a.GetRawValue(i) * b.GetRawValue(i);
            }
            return new DataColumn<int>($"{a.Name}*{b.Name}", data, nulls);
        }

        /// <summary>
        /// División real de dos columnas enteras. Devuelve DataColumn&lt;double&gt;.
        /// divisor == 0 → celda nula en el resultado (no lanza excepción), igual que double/double.
        /// Para división entera truncada usa AsDouble() + Math.Floor sobre el resultado.
        /// </summary>
        public static DataColumn<double> Divide(DataColumn<int> a, DataColumn<int> b)
        {
            ValidateSameLength(a, b);
            var (data, nulls) = Allocate(a.Length);
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i) || b.IsNull(i)) { nulls[i] = true; continue; }
                int divisor = b.GetRawValue(i);
                if (divisor == 0) { nulls[i] = true; continue; }   // div/0 → null, no excepción
                data[i] = (double)a.GetRawValue(i) / divisor;
            }
            return new DataColumn<double>($"{a.Name}/{b.Name}", data, nulls);
        }

        // ── int: columna OP escalar ───────────────────────────────────────────

        public static DataColumn<int> Add(DataColumn<int> col, int scalar)
        {
            var (data, nulls) = AllocateInt(col.Length);
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = col.GetRawValue(i) + scalar;
            }
            return new DataColumn<int>(col.Name, data, nulls);
        }

        public static DataColumn<int> Subtract(DataColumn<int> col, int scalar)
        {
            var (data, nulls) = AllocateInt(col.Length);
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = col.GetRawValue(i) - scalar;
            }
            return new DataColumn<int>(col.Name, data, nulls);
        }

        public static DataColumn<int> Multiply(DataColumn<int> col, int scalar)
        {
            var (data, nulls) = AllocateInt(col.Length);
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = col.GetRawValue(i) * scalar;
            }
            return new DataColumn<int>(col.Name, data, nulls);
        }

        /// <summary>
        /// División real de una columna entera por un escalar. Devuelve DataColumn&lt;double&gt;.
        /// scalar == 0 lanza DivideByZeroException (igual que double/escalar).
        /// </summary>
        public static DataColumn<double> Divide(DataColumn<int> col, int scalar)
        {
            if (scalar == 0) throw new DivideByZeroException(
                "Scalar divisor cannot be zero. Use column/column Divide if you need null propagation.");
            var (data, nulls) = Allocate(col.Length);
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = (double)col.GetRawValue(i) / scalar;
            }
            return new DataColumn<double>(col.Name, data, nulls);
        }

        // ── Conversión de tipo ────────────────────────────────────────────────

        /// <summary>
        /// Convierte DataColumn&lt;int&gt; a DataColumn&lt;double&gt;.
        /// Necesario para operaciones mixtas int+double.
        /// </summary>
        public static DataColumn<double> AsDouble(DataColumn<int> col)
        {
            var (data, nulls) = Allocate(col.Length);
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) { nulls[i] = true; continue; }
                data[i] = col.GetRawValue(i);
            }
            return new DataColumn<double>(col.Name, data, nulls);
        }

        // ── Agregaciones sobre una columna ────────────────────────────────────

        /// <summary>Suma ignorando nulls. Devuelve double.NaN si todos son null.</summary>
        public static double Sum(DataColumn<double> col)
        {
            double acc = 0;
            int count = 0;
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) continue;
                acc += col.GetRawValue(i);
                count++;
            }
            return count == 0 ? double.NaN : acc;
        }

        /// <summary>Media aritmética ignorando nulls. Devuelve double.NaN si todos son null.</summary>
        public static double Mean(DataColumn<double> col)
        {
            double acc = 0;
            int count = 0;
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) continue;
                acc += col.GetRawValue(i);
                count++;
            }
            return count == 0 ? double.NaN : acc / count;
        }

        /// <summary>Mínimo ignorando nulls. Devuelve double.NaN si todos son null.</summary>
        public static double Min(DataColumn<double> col)
        {
            double min = double.MaxValue;
            int count = 0;
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) continue;
                double v = col.GetRawValue(i);
                if (v < min) min = v;
                count++;
            }
            return count == 0 ? double.NaN : min;
        }

        /// <summary>Máximo ignorando nulls. Devuelve double.NaN si todos son null.</summary>
        public static double Max(DataColumn<double> col)
        {
            double max = double.MinValue;
            int count = 0;
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) continue;
                double v = col.GetRawValue(i);
                if (v > max) max = v;
                count++;
            }
            return count == 0 ? double.NaN : max;
        }

        /// <summary>
        /// Desviación estándar muestral (n-1) ignorando nulls.
        /// Devuelve double.NaN si hay menos de 2 valores no nulos.
        /// </summary>
        public static double Std(DataColumn<double> col)
        {
            double mean = Mean(col);
            if (double.IsNaN(mean)) return double.NaN;

            double sumSq = 0;
            int count = 0;
            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) continue;
                double diff = col.GetRawValue(i) - mean;
                sumSq += diff * diff;
                count++;
            }
            return count < 2 ? double.NaN : System.Math.Sqrt(sumSq / (count - 1));
        }

        /// <summary>Número de celdas no nulas.</summary>
        public static int Count(DataColumn<double> col)
        {
            int count = 0;
            for (int i = 0; i < col.Length; i++)
                if (!col.IsNull(i)) count++;
            return count;
        }

        // ── Helpers privados ──────────────────────────────────────────────────

        private static void ValidateSameLength(BaseColumn a, BaseColumn b)
        {
            if (a == null) throw new ArgumentNullException(nameof(a));
            if (b == null) throw new ArgumentNullException(nameof(b));
            if (a.Length != b.Length)
                throw new ArgumentException(
                    $"Column lengths must match: '{a.Name}' has {a.Length}, '{b.Name}' has {b.Length}.");
        }

        private static (double[] data, bool[] nulls) Allocate(int length)
            => (new double[length], new bool[length]);

        private static (int[] data, bool[] nulls) AllocateInt(int length)
            => (new int[length], new bool[length]);
    }
}