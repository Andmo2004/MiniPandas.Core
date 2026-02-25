using System;
using System.Collections.Generic;
using MiniPandas.Core.Columns;
using MiniPandas.Core.Operations;

namespace MiniPandas.Core.Operations.Merge
{
    /// <summary>
    /// Implementación del merge (join) entre dos DataFrames.
    ///
    /// ALGORITMO — hash join:
    ///   Fase 1 (build):  indexa el DataFrame derecho en un Dictionary&lt;RowKey, List&lt;int&gt;&gt;.
    ///   Fase 2 (probe):  itera el izquierdo, busca coincidencias y acumula pares de índices.
    ///   Fase 3 (gather): construye las columnas del resultado via ColumnGather.
    ///
    /// COLUMNAS NO-CLAVE:
    ///   Ahora delegan en ColumnGather.Gather, que centraliza el dispatch por tipo.
    ///   GatherMergedKeyColumn sigue siendo local porque su lógica (left-or-right fallback
    ///   para outer joins) no encaja en la interfaz genérica de ColumnGather.
    /// </summary>
    internal static class MergeOp
    {
        internal static DataFrame Execute(
            DataFrame left,
            DataFrame right,
            string[] leftOn,
            string[] rightOn,
            JoinType how)
        {
            ValidateKeys(left, leftOn, "left");
            ValidateKeys(right, rightOn, "right");

            var rightMap = BuildHashMap(right, rightOn);

            var lIdxList = new List<int>(left.RowCount);
            var rIdxList = new List<int>(left.RowCount);
            var matchedRight = new HashSet<int>();

            for (int li = 0; li < left.RowCount; li++)
            {
                RowKey? key = BuildKey(left, leftOn, li);

                if (key == null)
                {
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
            }

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

            return BuildResult(
                left, right,
                leftOn, rightOn,
                lIdxList.ToArray(),
                rIdxList.ToArray());
        }

        // ── Fase 1 ────────────────────────────────────────────────────────────

        private static Dictionary<RowKey, List<int>> BuildHashMap(DataFrame df, string[] keyNames)
        {
            var map = new Dictionary<RowKey, List<int>>();

            for (int ri = 0; ri < df.RowCount; ri++)
            {
                RowKey? key = BuildKey(df, keyNames, ri);
                if (key == null) continue;

                if (!map.TryGetValue(key.Value, out var list))
                {
                    list = new List<int>(1);
                    map[key.Value] = list;
                }
                list.Add(ri);
            }

            return map;
        }

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

        // ── Fase 3 ────────────────────────────────────────────────────────────

        private static DataFrame BuildResult(
            DataFrame left, DataFrame right,
            string[] leftOn, string[] rightOn,
            int[] lIdx, int[] rIdx)
        {
            var leftOnSet = new HashSet<string>(leftOn, StringComparer.OrdinalIgnoreCase);
            var rightOnSet = new HashSet<string>(rightOn, StringComparer.OrdinalIgnoreCase);

            var leftNonKeys = new List<string>();
            var rightNonKeys = new List<string>();

            foreach (var name in left.ColumnNames)
                if (!leftOnSet.Contains(name)) leftNonKeys.Add(name);

            foreach (var name in right.ColumnNames)
                if (!rightOnSet.Contains(name)) rightNonKeys.Add(name);

            var leftNonKeySet = new HashSet<string>(leftNonKeys, StringComparer.OrdinalIgnoreCase);
            var rightNonKeySet = new HashSet<string>(rightNonKeys, StringComparer.OrdinalIgnoreCase);

            var resultColumns = new List<BaseColumn>();

            // 1. Columnas clave — aparecen una sola vez con el nombre izquierdo.
            //    Lógica left-or-right para outer joins: permanece local en MergeOp.
            for (int k = 0; k < leftOn.Length; k++)
            {
                resultColumns.Add(GatherMergedKeyColumn(
                    left[leftOn[k]], right[rightOn[k]],
                    lIdx, rIdx,
                    leftOn[k]));
            }

            // 2. Columnas no-clave izquierdas — delegan en ColumnGather
            foreach (var name in leftNonKeys)
            {
                string resultName = rightNonKeySet.Contains(name) ? name + "_x" : name;
                resultColumns.Add(ColumnGather.Gather(left[name], lIdx, resultName));
            }

            // 3. Columnas no-clave derechas — delegan en ColumnGather
            foreach (var name in rightNonKeys)
            {
                string resultName = leftNonKeySet.Contains(name) ? name + "_y" : name;
                resultColumns.Add(ColumnGather.Gather(right[name], rIdx, resultName));
            }

            return new DataFrame(resultColumns);
        }

        /// <summary>
        /// Variante para columnas clave en outer joins:
        /// cuando lIdx[i] == -1, el valor se toma de rightCol en rIdx[i].
        /// Esta lógica es específica de Merge y no encaja en ColumnGather genérico.
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

        // ── RowKey ────────────────────────────────────────────────────────────

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
                int hash = 17;
                foreach (var v in _values)
                    hash = hash * 31 + (v?.GetHashCode() ?? 0);
                return hash;
            }
        }
    }
}