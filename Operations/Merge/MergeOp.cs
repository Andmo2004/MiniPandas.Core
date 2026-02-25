using System;
using System.Collections.Generic;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.Operations.Merge
{
    /// <summary>
    /// Implementación del merge (join) entre dos DataFrames.
    ///
    /// ALGORITMO — hash join:
    ///   Fase 1 (build):  indexa el DataFrame derecho en un Dictionary&lt;RowKey, List&lt;int&gt;&gt;.
    ///   Fase 2 (probe):  itera el izquierdo, busca coincidencias y acumula pares de índices.
    ///   Fase 3 (gather): construye las columnas del resultado a partir de esos pares.
    ///
    /// COMPLEJIDAD:
    ///   Tiempo: O(n + m) caso medio, O(n·m) caso peor (todas las filas con la misma clave).
    ///   Espacio: O(m) para el hash + O(|resultado|) para los pares de índices.
    ///   El caso peor es inevitable en cualquier join; en la práctica las claves son selectivas.
    ///
    /// CLAVES NULAS:
    ///   Una fila con cualquier valor de clave nulo no participa en el match.
    ///   En Left/Right/Outer, esa fila sí aparece en el resultado con null fill
    ///   en las columnas del lado sin datos. Consistente con pandas (na_action default).
    ///
    /// COLUMNAS EN EL RESULTADO:
    ///   - Columnas clave:    aparecen una sola vez con el nombre de la clave izquierda.
    ///   - Columnas no-clave: si un nombre existe en ambos lados, recibe sufijo "_x" (izq)
    ///                        y "_y" (der), igual que pandas con suffixes=('_x','_y').
    ///
    /// TIPOS SOPORTADOS EN GatherColumn:
    ///   DataColumn&lt;double&gt;, DataColumn&lt;int&gt;, DataColumn&lt;DateTime&gt;, DataColumn&lt;bool&gt;,
    ///   StringColumn, CategoricalColumn.
    ///   Para añadir un nuevo tipo: añade un bloque 'if' en GatherColumn y
    ///   en GatherMergedKeyColumn con la misma estructura que los existentes.
    /// </summary>
    internal static class MergeOp
    {
        // ── Punto de entrada ──────────────────────────────────────────────────

        internal static DataFrame Execute(
            DataFrame left,
            DataFrame right,
            string[] leftOn,
            string[] rightOn,
            JoinType how)
        {
            ValidateKeys(left, leftOn, "left");
            ValidateKeys(right, rightOn, "right");

            // Fase 1: build — indexar el DataFrame derecho por clave
            var rightMap = BuildHashMap(right, rightOn);

            // Fase 2: probe — acumular pares (lIdx, rIdx)
            // -1 en cualquiera de los dos = sin pareja (null fill en esa columna)
            var lIdxList = new List<int>(left.RowCount);
            var rIdxList = new List<int>(left.RowCount);
            var matchedRight = new HashSet<int>();

            for (int li = 0; li < left.RowCount; li++)
            {
                RowKey? key = BuildKey(left, leftOn, li);

                if (key == null)
                {
                    // Clave nula: no participa en el match
                    if (how == JoinType.Left || how == JoinType.Outer)
                    {
                        lIdxList.Add(li);
                        rIdxList.Add(-1);
                    }
                    continue;
                }

                if (rightMap.TryGetValue(key.Value, out var matches))
                {
                    foreach (int ri in matches)
                    {
                        lIdxList.Add(li);
                        rIdxList.Add(ri);
                        matchedRight.Add(ri);
                    }
                }
                else if (how == JoinType.Left || how == JoinType.Outer)
                {
                    lIdxList.Add(li);
                    rIdxList.Add(-1);
                }
                // Inner / Right: filas izquierdas sin pareja se descartan
            }

            // Filas derechas sin coincidencia (Right / Outer)
            if (how == JoinType.Right || how == JoinType.Outer)
            {
                for (int ri = 0; ri < right.RowCount; ri++)
                {
                    if (!matchedRight.Contains(ri))
                    {
                        lIdxList.Add(-1);
                        rIdxList.Add(ri);
                    }
                }
            }

            // Fase 3: gather — construir columnas del resultado
            return BuildResult(
                left, right,
                leftOn, rightOn,
                lIdxList.ToArray(),
                rIdxList.ToArray());
        }

        // ── Fase 1: construcción del hash map ─────────────────────────────────

        private static Dictionary<RowKey, List<int>> BuildHashMap(DataFrame df, string[] keyNames)
        {
            var map = new Dictionary<RowKey, List<int>>();

            for (int ri = 0; ri < df.RowCount; ri++)
            {
                RowKey? key = BuildKey(df, keyNames, ri);
                if (key == null) continue;   // clave nula → no indexar

                if (!map.TryGetValue(key.Value, out var list))
                {
                    list = new List<int>(1);
                    map[key.Value] = list;
                }
                list.Add(ri);
            }

            return map;
        }

        /// <summary>
        /// Construye la clave de una fila a partir de las columnas indicadas.
        /// Devuelve null si cualquier valor de clave es nulo
        /// (esa fila no participará en el match).
        ///
        /// NOTA DE RENDIMIENTO:
        ///   GetBoxed() sobre StringColumn y CategoricalColumn devuelve string,
        ///   que es un tipo referencia — no hay boxing en el sentido de C#.
        ///   El coste real es la asignación de new object[keyNames.Length] por fila.
        ///   Para joins de clave simple (el caso más común) esto es 1 objeto pequeño
        ///   por fila; el GC lo recolecta en gen0. Si el profiler lo señala como
        ///   hotspot, considera especializar RowKey para el caso de 1 y 2 claves
        ///   con campos directamente en el struct (sin array interno).
        /// </summary>
        private static RowKey? BuildKey(DataFrame df, string[] keyNames, int row)
        {
            var values = new object[keyNames.Length];

            for (int k = 0; k < keyNames.Length; k++)
            {
                var col = df[keyNames[k]];
                if (col.IsNull(row)) return null;
                values[k] = col.GetBoxed(row);
            }

            return new RowKey(values);
        }

        // ── Fase 3: construcción del resultado ────────────────────────────────

        private static DataFrame BuildResult(
            DataFrame left, DataFrame right,
            string[] leftOn, string[] rightOn,
            int[] lIdx, int[] rIdx)
        {
            var leftOnSet = new HashSet<string>(leftOn, StringComparer.OrdinalIgnoreCase);
            var rightOnSet = new HashSet<string>(rightOn, StringComparer.OrdinalIgnoreCase);

            // Columnas no-clave de cada lado en orden de inserción original
            var leftNonKeys = new List<string>();
            var rightNonKeys = new List<string>();

            foreach (var name in left.ColumnNames)
                if (!leftOnSet.Contains(name)) leftNonKeys.Add(name);

            foreach (var name in right.ColumnNames)
                if (!rightOnSet.Contains(name)) rightNonKeys.Add(name);

            // Sets para detectar conflictos de nombre entre no-claves
            var leftNonKeySet = new HashSet<string>(leftNonKeys, StringComparer.OrdinalIgnoreCase);
            var rightNonKeySet = new HashSet<string>(rightNonKeys, StringComparer.OrdinalIgnoreCase);

            var resultColumns = new List<BaseColumn>();

            // 1. Columnas clave — aparecen una sola vez con el nombre izquierdo
            //    GatherMergedKeyColumn maneja el caso outer donde lIdx[i] puede ser -1
            for (int k = 0; k < leftOn.Length; k++)
            {
                resultColumns.Add(GatherMergedKeyColumn(
                    left[leftOn[k]], right[rightOn[k]],
                    lIdx, rIdx,
                    leftOn[k]));
            }

            // 2. Columnas no-clave izquierdas
            foreach (var name in leftNonKeys)
            {
                string resultName = rightNonKeySet.Contains(name) ? name + "_x" : name;
                resultColumns.Add(GatherColumn(left[name], lIdx, resultName));
            }

            // 3. Columnas no-clave derechas
            foreach (var name in rightNonKeys)
            {
                string resultName = leftNonKeySet.Contains(name) ? name + "_y" : name;
                resultColumns.Add(GatherColumn(right[name], rIdx, resultName));
            }

            return new DataFrame(resultColumns);
        }

        // ── GatherColumn — reconstruye una columna desde índices arbitrarios ──

        /// <summary>
        /// Construye una nueva columna del mismo tipo que <paramref name="source"/>
        /// recogiendo los valores en las posiciones indicadas por <paramref name="indices"/>.
        /// indices[i] == -1 → celda nula (fila sin pareja en left/right/outer join).
        /// </summary>
        private static BaseColumn GatherColumn(BaseColumn source, int[] indices, string name)
        {
            int n = indices.Length;

            if (source is DataColumn<double> dcD)
            {
                var data = new double[n]; var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1 || dcD.IsNull(indices[i])) { nulls[i] = true; continue; }
                    data[i] = dcD.GetRawValue(indices[i]);
                }
                return new DataColumn<double>(name, data, nulls);
            }

            if (source is DataColumn<int> dcI)
            {
                var data = new int[n]; var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1 || dcI.IsNull(indices[i])) { nulls[i] = true; continue; }
                    data[i] = dcI.GetRawValue(indices[i]);
                }
                return new DataColumn<int>(name, data, nulls);
            }

            if (source is DataColumn<DateTime> dcDt)
            {
                var data = new DateTime[n]; var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1 || dcDt.IsNull(indices[i])) { nulls[i] = true; continue; }
                    data[i] = dcDt.GetRawValue(indices[i]);
                }
                return new DataColumn<DateTime>(name, data, nulls);
            }

            if (source is DataColumn<bool> dcB)
            {
                var data = new bool[n]; var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1 || dcB.IsNull(indices[i])) { nulls[i] = true; continue; }
                    data[i] = dcB.GetRawValue(indices[i]);
                }
                return new DataColumn<bool>(name, data, nulls);
            }

            if (source is StringColumn sc)
            {
                var data = new string[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1) continue;   // null permanece null
                    data[i] = sc[indices[i]];
                }
                return new StringColumn(name, data);
            }

            if (source is CategoricalColumn cc)
            {
                // GatherByIndices reutiliza el diccionario de categorías por referencia
                // y mapea códigos directamente — sin decode a string ni recode de vuelta.
                return cc.GatherByIndices(name, indices);
            }

            throw new InvalidOperationException(
                $"GatherColumn: unsupported column type '{source.GetType().Name}' " +
                $"for column '{source.Name}'. " +
                $"Add an 'if' block in GatherColumn and GatherMergedKeyColumn.");
        }

        /// <summary>
        /// Variante para columnas clave en Outer/Right joins:
        /// cuando lIdx[i] == -1 (fila derecha sin pareja izquierda), el valor
        /// de la clave se toma de <paramref name="rightCol"/> en rIdx[i].
        ///
        /// Ambas columnas deben tener tipos compatibles; de lo contrario se lanza
        /// InvalidOperationException porque un join sobre claves de distinto tipo
        /// es un error semántico del llamador.
        ///
        /// Excepción: String y Categorical son compatibles entre sí — ambas
        /// representan texto y el resultado se construye como el tipo de la izquierda.
        /// </summary>
        private static BaseColumn GatherMergedKeyColumn(
            BaseColumn leftCol, BaseColumn rightCol,
            int[] lIdx, int[] rIdx,
            string name)
        {
            int n = lIdx.Length;

            if (leftCol is DataColumn<double> lD && rightCol is DataColumn<double> rD)
            {
                var data = new double[n]; var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    bool useLeft = lIdx[i] != -1;
                    DataColumn<double> col = useLeft ? lD : rD;
                    int idx = useLeft ? lIdx[i] : rIdx[i];
                    if (col.IsNull(idx)) { nulls[i] = true; continue; }
                    data[i] = col.GetRawValue(idx);
                }
                return new DataColumn<double>(name, data, nulls);
            }

            if (leftCol is DataColumn<int> lI && rightCol is DataColumn<int> rI)
            {
                var data = new int[n]; var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    bool useLeft = lIdx[i] != -1;
                    DataColumn<int> col = useLeft ? lI : rI;
                    int idx = useLeft ? lIdx[i] : rIdx[i];
                    if (col.IsNull(idx)) { nulls[i] = true; continue; }
                    data[i] = col.GetRawValue(idx);
                }
                return new DataColumn<int>(name, data, nulls);
            }

            if (leftCol is DataColumn<DateTime> lDt && rightCol is DataColumn<DateTime> rDt)
            {
                var data = new DateTime[n]; var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    bool useLeft = lIdx[i] != -1;
                    DataColumn<DateTime> col = useLeft ? lDt : rDt;
                    int idx = useLeft ? lIdx[i] : rIdx[i];
                    if (col.IsNull(idx)) { nulls[i] = true; continue; }
                    data[i] = col.GetRawValue(idx);
                }
                return new DataColumn<DateTime>(name, data, nulls);
            }

            if (leftCol is DataColumn<bool> lB && rightCol is DataColumn<bool> rB)
            {
                var data = new bool[n]; var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    bool useLeft = lIdx[i] != -1;
                    DataColumn<bool> col = useLeft ? lB : rB;
                    int idx = useLeft ? lIdx[i] : rIdx[i];
                    if (col.IsNull(idx)) { nulls[i] = true; continue; }
                    data[i] = col.GetRawValue(idx);
                }
                return new DataColumn<bool>(name, data, nulls);
            }

            // String y Categorical son compatibles: ambas representan texto
            if ((leftCol is StringColumn || leftCol is CategoricalColumn) &&
                (rightCol is StringColumn || rightCol is CategoricalColumn))
            {
                var data = new string[n];
                for (int i = 0; i < n; i++)
                {
                    bool useLeft = lIdx[i] != -1;
                    BaseColumn col = useLeft ? leftCol : rightCol;
                    int idx = useLeft ? lIdx[i] : rIdx[i];
                    if (!col.IsNull(idx))
                        data[i] = (string)col.GetBoxed(idx);
                }
                // Preservar CategoricalColumn si el lado izquierdo lo era
                return leftCol is CategoricalColumn
                    ? (BaseColumn)new CategoricalColumn(name, data)
                    : new StringColumn(name, data);
            }

            throw new InvalidOperationException(
                $"GatherMergedKeyColumn: incompatible key column types " +
                $"'{leftCol.GetType().Name}' (left) and '{rightCol.GetType().Name}' (right) " +
                $"for key '{name}'. Key columns on both sides must have the same type.");
        }

        // ── Validaciones ──────────────────────────────────────────────────────

        private static void ValidateKeys(DataFrame df, string[] keys, string side)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentException(
                    $"At least one key column is required for the {side} DataFrame.",
                    nameof(keys));

            foreach (var key in keys)
            {
                if (string.IsNullOrWhiteSpace(key))
                    throw new ArgumentException(
                        $"Key column names cannot be null or empty ({side}).");

                if (!df.ContainsColumn(key))
                    throw new ArgumentException(
                        $"Key column '{key}' does not exist in the {side} DataFrame.");
            }
        }

        // ── RowKey — clave compuesta con igualdad y hash correctos ────────────

        /// <summary>
        /// Clave compuesta para el hash join.
        /// Almacena los valores boxeados de las columnas clave y define
        /// igualdad elemento a elemento y hash combinado.
        ///
        /// Los valores null no deberían llegar aquí — BuildKey los filtra antes.
        /// Si llegaran, object.Equals(null, null) == true y hash = 0, lo que
        /// daría matches incorrectos entre filas con claves nulas; por eso BuildKey
        /// descarta esas filas antes de construir el RowKey.
        /// </summary>
        private readonly struct RowKey : IEquatable<RowKey>
        {
            private readonly object[] _values;

            internal RowKey(object[] values) => _values = values;

            public bool Equals(RowKey other)
            {
                if (_values.Length != other._values.Length) return false;
                for (int i = 0; i < _values.Length; i++)
                    if (!object.Equals(_values[i], other._values[i])) return false;
                return true;
            }

            public override bool Equals(object obj) =>
                obj is RowKey other && Equals(other);

            public override int GetHashCode()
            {
                // Combinación clásica con multiplicador primo
                // Produce distribución uniforme para las combinaciones más comunes
                int hash = 17;
                foreach (var v in _values)
                    hash = hash * 31 + (v?.GetHashCode() ?? 0);
                return hash;
            }
        }
    }
}