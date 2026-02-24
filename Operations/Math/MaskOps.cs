using System;

namespace MiniPandas.Core.Operations.Math
{
    /// <summary>
    /// Operaciones sobre máscaras booleanas (bool[]).
    ///
    /// Las máscaras son el pegamento entre comparaciones y filtrado:
    ///   bool[] mask = col.GreaterThan(100);        // comparación
    ///   mask = MaskOps.And(mask, col2.IsNotNull()); // combinación
    ///   df.Where(mask);                             // filtrado
    ///
    /// Todas las operaciones validan que los arrays tengan la misma longitud.
    /// Todas devuelven un nuevo array (inmutable).
    /// </summary>
    public static class MaskOps
    {
        /// <summary>
        /// AND lógico elemento a elemento.
        /// true solo si ambos son true.
        /// Equivalente a: mask1 &amp; mask2 en pandas.
        /// </summary>
        public static bool[] And(bool[] a, bool[] b)
        {
            ValidateSameLength(a, b);
            var result = new bool[a.Length];
            for (int i = 0; i < a.Length; i++)
                result[i] = a[i] && b[i];
            return result;
        }

        /// <summary>
        /// OR lógico elemento a elemento.
        /// true si alguno es true.
        /// Equivalente a: mask1 | mask2 en pandas.
        /// </summary>
        public static bool[] Or(bool[] a, bool[] b)
        {
            ValidateSameLength(a, b);
            var result = new bool[a.Length];
            for (int i = 0; i < a.Length; i++)
                result[i] = a[i] || b[i];
            return result;
        }

        /// <summary>
        /// NOT lógico elemento a elemento.
        /// Invierte cada valor de la máscara.
        /// Equivalente a: ~mask en pandas.
        /// </summary>
        public static bool[] Not(bool[] mask)
        {
            if (mask == null) throw new ArgumentNullException(nameof(mask));
            var result = new bool[mask.Length];
            for (int i = 0; i < mask.Length; i++)
                result[i] = !mask[i];
            return result;
        }

        /// <summary>
        /// XOR lógico elemento a elemento.
        /// true si exactamente uno de los dos es true.
        /// </summary>
        public static bool[] Xor(bool[] a, bool[] b)
        {
            ValidateSameLength(a, b);
            var result = new bool[a.Length];
            for (int i = 0; i < a.Length; i++)
                result[i] = a[i] ^ b[i];
            return result;
        }

        /// <summary>
        /// Combina múltiples máscaras con AND.
        /// Útil para filtros con muchas condiciones sin anidar llamadas.
        /// MaskOps.All(mask1, mask2, mask3) equivale a mask1 &amp; mask2 &amp; mask3.
        /// </summary>
        public static bool[] All(params bool[][] masks)
        {
            if (masks == null || masks.Length == 0)
                throw new ArgumentException("At least one mask required.", nameof(masks));

            var result = (bool[])masks[0].Clone();
            for (int m = 1; m < masks.Length; m++)
            {
                ValidateSameLength(result, masks[m]);
                for (int i = 0; i < result.Length; i++)
                    result[i] = result[i] && masks[m][i];
            }
            return result;
        }

        /// <summary>
        /// Combina múltiples máscaras con OR.
        /// MaskOps.Any(mask1, mask2, mask3) equivale a mask1 | mask2 | mask3.
        /// </summary>
        public static bool[] Any(params bool[][] masks)
        {
            if (masks == null || masks.Length == 0)
                throw new ArgumentException("At least one mask required.", nameof(masks));

            var result = (bool[])masks[0].Clone();
            for (int m = 1; m < masks.Length; m++)
            {
                ValidateSameLength(result, masks[m]);
                for (int i = 0; i < result.Length; i++)
                    result[i] = result[i] || masks[m][i];
            }
            return result;
        }

        // ── Estadísticas sobre máscaras ───────────────────────────────────────

        /// <summary>Número de valores true en la máscara.</summary>
        public static int CountTrue(bool[] mask)
        {
            if (mask == null) throw new ArgumentNullException(nameof(mask));
            int count = 0;
            for (int i = 0; i < mask.Length; i++)
                if (mask[i]) count++;
            return count;
        }

        /// <summary>
        /// Devuelve los índices donde la máscara es true.
        /// Útil para depuración y para operaciones que necesitan los índices explícitos.
        /// </summary>
        public static int[] TrueIndices(bool[] mask)
        {
            if (mask == null) throw new ArgumentNullException(nameof(mask));
            int count = CountTrue(mask);
            var indices = new int[count];
            int j = 0;
            for (int i = 0; i < mask.Length; i++)
                if (mask[i]) indices[j++] = i;
            return indices;
        }

        // ── Helper privado ────────────────────────────────────────────────────

        private static void ValidateSameLength(bool[] a, bool[] b)
        {
            if (a == null) throw new ArgumentNullException(nameof(a));
            if (b == null) throw new ArgumentNullException(nameof(b));
            if (a.Length != b.Length)
                throw new ArgumentException(
                    $"Mask lengths must match: {a.Length} vs {b.Length}.");
        }
    }
}