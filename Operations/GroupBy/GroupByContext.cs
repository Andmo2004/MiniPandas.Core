using System;
using System.Collections.Generic;
using System.Linq;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.Operations.GroupBy
{
    /// <summary>
    /// Resultado intermedio de DataFrame.GroupBy().
    ///
    /// DISEÑO — dos fases separadas (igual que pandas):
    ///
    ///   FASE 1 — Agrupación (constructor):
    ///     Recorre las filas UNA sola vez y construye un diccionario
    ///     clave → List&lt;int&gt; (índices de filas que pertenecen a ese grupo).
    ///     No copia datos: solo índices. O(n).
    ///
    ///   FASE 2 — Materialización (Agg / Count / Filter / Apply):
    ///     Recorre los grupos y aplica la función de agregación sobre
    ///     los índices de cada grupo. Solo aquí se tocan los datos reales.
    ///
    /// CLAVES COMPUESTAS:
    ///   GroupBy("pais", "ciudad") construye claves como "España|Madrid".
    ///   El separador "|" es poco común en datos reales; si tus datos
    ///   pueden contenerlo, cámbialo por KeySeparator.
    ///
    /// SOPORTE PARA CategoricalColumn:
    ///   Si la columna de agrupación es CategoricalColumn, agrupa por código
    ///   entero directamente (sin comparar strings). Más rápido en columnas
    ///   con muchas filas y pocas categorías.
    ///
    /// ALMACENAMIENTO DE ÍNDICES — dos pasadas:
    ///   Pasada 1: contar cuántas filas pertenecen a cada grupo.
    ///   Pasada 2: asignar arrays exactos y rellenar posición a posición.
    ///   Esto evita las reallocations internas de List&lt;int&gt; cuando los grupos
    ///   son grandes (List duplica su buffer interno cada vez que se llena).
    /// </summary>
    public class GroupByContext
    {
        private readonly DataFrame _source;
        private readonly string[] _keys;

        // Mapa de clave de grupo → índices de filas (array de tamaño exacto)
        private readonly Dictionary<string, int[]> _groups;

        // Separador para claves compuestas. Cambia si tus datos contienen "|".
        public static string KeySeparator { get; set; } = "|";

        // ── Constructor — Fase 1: agrupación ─────────────────────────────────

        internal GroupByContext(DataFrame source, string[] keys)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (keys == null || keys.Length == 0)
                throw new ArgumentException("At least one key column required.", nameof(keys));

            foreach (var key in keys)
                if (!source.ContainsColumn(key))
                    throw new KeyNotFoundException(
                        $"Key column '{key}' not found in DataFrame.");

            _source = source;
            _keys = keys;
            _groups = BuildGroups(source, keys);
        }

        /// <summary>
        /// Construye el mapa de grupos en dos pasadas para evitar reallocations.
        ///
        /// Pasada 1: asignar una clave a cada fila y contar cuántas filas
        ///           pertenecen a cada grupo.
        /// Pasada 2: con los conteos exactos, asignar arrays del tamaño justo
        ///           y rellenar los índices.
        /// </summary>
        private static Dictionary<string, int[]> BuildGroups(DataFrame source, string[] keys)
        {
            var keyColumns = new BaseColumn[keys.Length];
            for (int k = 0; k < keys.Length; k++)
                keyColumns[k] = source[keys[k]];

            // ── Pasada 1: calcular la clave de cada fila y contar por grupo ──
            var rowKeys = new string[source.RowCount];
            var counts = new Dictionary<string, int>(StringComparer.Ordinal);

            for (int row = 0; row < source.RowCount; row++)
            {
                string key = BuildGroupKey(keyColumns, row);
                rowKeys[row] = key;

                if (counts.TryGetValue(key, out int c))
                    counts[key] = c + 1;
                else
                    counts[key] = 1;
            }

            // ── Asignar arrays de tamaño exacto ──────────────────────────────
            var groups = new Dictionary<string, int[]>(counts.Count, StringComparer.Ordinal);
            var cursors = new Dictionary<string, int>(counts.Count, StringComparer.Ordinal);

            foreach (var kvp in counts)
            {
                groups[kvp.Key] = new int[kvp.Value];   // tamaño exacto, sin reserva extra
                cursors[kvp.Key] = 0;
            }

            // ── Pasada 2: rellenar los arrays con los índices de fila ─────────
            for (int row = 0; row < source.RowCount; row++)
            {
                string key = rowKeys[row];
                int pos = cursors[key];
                groups[key][pos] = row;
                cursors[key] = pos + 1;
            }

            return groups;
        }

        private static string BuildGroupKey(BaseColumn[] keyColumns, int row)
        {
            if (keyColumns.Length == 1)
                return GetKeyValue(keyColumns[0], row);

            var parts = new string[keyColumns.Length];
            for (int k = 0; k < keyColumns.Length; k++)
                parts[k] = GetKeyValue(keyColumns[k], row);

            return string.Join(KeySeparator, parts);
        }

        private static string GetKeyValue(BaseColumn col, int row)
        {
            if (col is CategoricalColumn cc)
                return cc.GetCode(row).ToString();

            var val = col.GetBoxed(row);
            return val == null ? "\0null" : val.ToString();
        }

        // ── Propiedades de inspección ─────────────────────────────────────────

        /// <summary>Número de grupos distintos.</summary>
        public int GroupCount => _groups.Count;

        /// <summary>
        /// Número de filas por grupo.
        /// Útil para depuración antes de materializar con Agg().
        /// </summary>
        public Dictionary<string, int> GroupSizes()
        {
            var sizes = new Dictionary<string, int>(_groups.Count, StringComparer.Ordinal);
            foreach (var kvp in _groups)
                sizes[kvp.Key] = kvp.Value.Length;
            return sizes;
        }

        // ── Fase 2: materialización ───────────────────────────────────────────

        /// <summary>
        /// Aplica funciones de agregación por columna y devuelve un nuevo DataFrame.
        /// El DataFrame resultado tiene una columna por clave + una por cada agregación.
        /// Una fila por grupo.
        /// </summary>
        public DataFrame Agg(Dictionary<string, AggFunc> aggregations)
        {
            if (aggregations == null) throw new ArgumentNullException(nameof(aggregations));
            if (aggregations.Count == 0)
                throw new ArgumentException("At least one aggregation required.", nameof(aggregations));

            foreach (var colName in aggregations.Keys)
                if (!_source.ContainsColumn(colName))
                    throw new KeyNotFoundException(
                        $"Aggregation column '{colName}' not found in DataFrame.");

            int groupCount = _groups.Count;
            var groupList = _groups.ToList();   // orden estable

            // ── Construir columnas clave ──────────────────────────────────────
            var keyColumns = new BaseColumn[_keys.Length];
            for (int k = 0; k < _keys.Length; k++)
                keyColumns[k] = _source[_keys[k]];

            var keyData = new string[_keys.Length][];
            for (int k = 0; k < _keys.Length; k++)
                keyData[k] = new string[groupCount];

            for (int g = 0; g < groupCount; g++)
            {
                int sampleRow = groupList[g].Value[0];
                for (int k = 0; k < _keys.Length; k++)
                    keyData[k][g] = keyColumns[k].IsNull(sampleRow)
                        ? null
                        : DecodeKeyValue(keyColumns[k], sampleRow);
            }

            // ── Construir columnas de agregación ──────────────────────────────
            var aggResults = new Dictionary<string, object[]>(aggregations.Count);
            foreach (var colName in aggregations.Keys)
                aggResults[colName] = new object[groupCount];

            for (int g = 0; g < groupCount; g++)
            {
                int[] indices = groupList[g].Value;
                foreach (var kvp in aggregations)
                    aggResults[kvp.Key][g] = Aggregations.Aggregate(
                        _source[kvp.Key], indices, kvp.Value);
            }

            // ── Ensamblar DataFrame resultado — tamaño exacto conocido ────────
            int totalCols = _keys.Length + aggregations.Count;
            var resultColumns = new BaseColumn[totalCols];
            int colIdx = 0;

            for (int k = 0; k < _keys.Length; k++)
                resultColumns[colIdx++] = new StringColumn(_keys[k], keyData[k]);

            foreach (var kvp in aggregations)
                resultColumns[colIdx++] = BuildAggColumn(kvp.Key, kvp.Value, aggResults[kvp.Key], groupCount);

            return new DataFrame(resultColumns);
        }

        /// <summary>
        /// Conteo de filas por grupo. Equivalente a pandas .groupby().size().
        /// Devuelve un DataFrame con las columnas clave + columna "count".
        /// </summary>
        public DataFrame Count()
        {
            int groupCount = _groups.Count;
            var groupList = _groups.ToList();

            var keyColumns = new BaseColumn[_keys.Length];
            for (int k = 0; k < _keys.Length; k++)
                keyColumns[k] = _source[_keys[k]];

            var keyData = new string[_keys.Length][];
            for (int k = 0; k < _keys.Length; k++)
                keyData[k] = new string[groupCount];

            var counts = new double[groupCount];
            var nulls = new bool[groupCount];   // todos false: Count nunca es null

            for (int g = 0; g < groupCount; g++)
            {
                int sampleRow = groupList[g].Value[0];
                for (int k = 0; k < _keys.Length; k++)
                    keyData[k][g] = keyColumns[k].IsNull(sampleRow)
                        ? null
                        : DecodeKeyValue(keyColumns[k], sampleRow);

                counts[g] = groupList[g].Value.Length;
            }

            // Tamaño exacto conocido: _keys.Length + 1 columna de count
            var resultColumns = new BaseColumn[_keys.Length + 1];
            for (int k = 0; k < _keys.Length; k++)
                resultColumns[k] = new StringColumn(_keys[k], keyData[k]);

            resultColumns[_keys.Length] = new DataColumn<double>("count", counts, nulls);

            return new DataFrame(resultColumns);
        }

        /// <summary>
        /// Filtra grupos enteros según una condición (equivalente al HAVING de SQL).
        /// Evalúa cada grupo materializado; si el predicado devuelve true,
        /// todas las filas originales de ese grupo se incluyen en el resultado.
        /// </summary>
        public DataFrame Filter(Func<DataFrame, bool> predicate)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            // Máscara global para saber qué filas sobrevivirán al filtro
            var globalMask = new bool[_source.RowCount];

            foreach (var kvp in _groups)
            {
                // 1. Materializar el grupo actual (igual que hacemos en Apply)
                var groupMask = new bool[_source.RowCount];
                foreach (int idx in kvp.Value)
                    groupMask[idx] = true;

                var groupDf = _source.Where(groupMask);

                // 2. Pasar el DataFrame del grupo a la función del usuario
                bool keepGroup = predicate(groupDf);

                // 3. Si el grupo pasa el filtro, marcamos sus filas para conservarlas
                if (keepGroup)
                {
                    foreach (int idx in kvp.Value)
                        globalMask[idx] = true;
                }
            }

            // 4. Devolver un nuevo DataFrame aplicando la máscara global
            return _source.Where(globalMask);
        }

        /// <summary>
        /// Calcula una agregación por grupo y devuelve una nueva columna del MISMO TAMAÑO
        /// que el DataFrame original. El valor agregado se repite para todas las filas del grupo.
        /// </summary>
        public BaseColumn Transform(string columnName, AggFunc func)
        {
            if (!_source.ContainsColumn(columnName))
                throw new KeyNotFoundException(
                    $"Transform column '{columnName}' not found in DataFrame.");

            int totalRows = _source.RowCount;
            var rawResults = new object[totalRows];
            var column = _source[columnName];

            // 1. Recorremos los grupos
            foreach (var kvp in _groups)
            {
                int[] indices = kvp.Value;

                // 2. Calculamos el valor agregado solo para este grupo
                object groupValue = Aggregations.Aggregate(column, indices, func);

                // 3. "Repartimos" o "Broadcasteamos" ese valor a TODAS las filas del grupo
                foreach (int idx in indices)
                    rawResults[idx] = groupValue;
            }

            // 4. Construimos la columna final
            string newColName = $"{columnName}_{func.ToString().ToLower()}";
            return BuildTransformColumn(newColName, func, rawResults, totalRows, column);
        }

        /// <summary>
        /// Aplica una función arbitraria a cada grupo y concatena los resultados.
        /// ATENCIÓN: flexible pero lento. Para agregaciones estándar usa Agg().
        /// </summary>
        public DataFrame Apply(Func<DataFrame, DataFrame> func)
        {
            if (func == null) throw new ArgumentNullException(nameof(func));

            // Capacidad máxima conocida: _groups.Count grupos.
            // Puede haber menos si func devuelve null en algunos grupos.
            var resultParts = new List<DataFrame>(_groups.Count);

            foreach (var kvp in _groups)
            {
                var mask = new bool[_source.RowCount];
                foreach (int idx in kvp.Value)
                    mask[idx] = true;

                var groupDf = _source.Where(mask);
                var result = func(groupDf);
                if (result != null)
                    resultParts.Add(result);
            }

            if (resultParts.Count == 0)
                return new DataFrame(0);

            return Concat(resultParts);
        }

        // ── Helpers privados ──────────────────────────────────────────────────

        private static string DecodeKeyValue(BaseColumn col, int row)
        {
            if (col is CategoricalColumn cc)
                return cc.DecodeCategory(cc.GetCode(row));
            return col.GetBoxed(row)?.ToString();
        }

        private static BaseColumn BuildAggColumn(
            string name, AggFunc func, object[] rawResults, int rowCount)
        {
            if (func == AggFunc.Count)
            {
                var data = new int[rowCount];
                var nulls = new bool[rowCount];
                for (int i = 0; i < rowCount; i++)
                {
                    if (rawResults[i] == null) { nulls[i] = true; continue; }
                    data[i] = Convert.ToInt32(rawResults[i]);
                }
                return new DataColumn<int>(name, data, nulls);
            }
            else
            {
                var data = new double[rowCount];
                var nulls = new bool[rowCount];
                for (int i = 0; i < rowCount; i++)
                {
                    if (rawResults[i] == null) { nulls[i] = true; continue; }
                    data[i] = Convert.ToDouble(rawResults[i]);
                }
                return new DataColumn<double>(name, data, nulls);
            }
        }

        /// <summary>
        /// Concatena DataFrames verticalmente. Usado internamente por Apply().
        /// </summary>
        private static DataFrame Concat(List<DataFrame> parts)
        {
            var first = parts[0];
            int totalRows = 0;
            for (int p = 0; p < parts.Count; p++)
                totalRows += parts[p].RowCount;

            var columnNames = first.ColumnNames.ToArray();
            var resultColumns = new BaseColumn[columnNames.Length];

            for (int c = 0; c < columnNames.Length; c++)
            {
                var allBoxed = new object[totalRows];
                int offset = 0;

                for (int p = 0; p < parts.Count; p++)
                {
                    var part = parts[p];
                    if (!part.ContainsColumn(columnNames[c]))
                        throw new InvalidOperationException(
                            $"Apply result is missing column '{columnNames[c]}'.");

                    var col = part[columnNames[c]];
                    for (int r = 0; r < part.RowCount; r++)
                        allBoxed[offset++] = col.GetBoxed(r);
                }

                resultColumns[c] = BuildColumnFromBoxed(columnNames[c], allBoxed, first[columnNames[c]]);
            }

            return new DataFrame(resultColumns);
        }

        private static BaseColumn BuildColumnFromBoxed(
            string name, object[] boxed, BaseColumn templateColumn)
        {
            if (templateColumn is DataColumn<double> || templateColumn is DataColumn<int>)
            {
                var data = new double[boxed.Length];
                var nulls = new bool[boxed.Length];
                for (int i = 0; i < boxed.Length; i++)
                {
                    if (boxed[i] == null) { nulls[i] = true; continue; }
                    data[i] = Convert.ToDouble(boxed[i]);
                }
                return new DataColumn<double>(name, data, nulls);
            }

            if (templateColumn is CategoricalColumn)
            {
                var data = new string[boxed.Length];
                for (int i = 0; i < boxed.Length; i++)
                    data[i] = boxed[i]?.ToString();
                return new CategoricalColumn(name, data);
            }

            // StringColumn y cualquier otro tipo
            var strData = new string[boxed.Length];
            for (int i = 0; i < boxed.Length; i++)
                strData[i] = boxed[i]?.ToString();
            return new StringColumn(name, strData);
        }
        // ── Helper para Transform ─────────────────────────────────────────────

        private static BaseColumn BuildTransformColumn(
            string name, AggFunc func, object[] rawResults, int rowCount, BaseColumn originalCol)
        {
            // Count y NUnique devuelven enteros
            if (func == AggFunc.Count || func == AggFunc.NUnique)
            {
                var data = new int[rowCount];
                var nulls = new bool[rowCount];
                for (int i = 0; i < rowCount; i++)
                {
                    if (rawResults[i] == null) { nulls[i] = true; continue; }
                    data[i] = Convert.ToInt32(rawResults[i]);
                }
                return new DataColumn<int>(name, data, nulls);
            }

            // First y Last respetan el tipo de la columna original
            if (func == AggFunc.First || func == AggFunc.Last)
                return BuildColumnFromBoxed(name, rawResults, originalCol);

            // El resto (Sum, Mean, Min, Max, Std, Var, Prod, Median) devuelven double
            var dataDouble = new double[rowCount];
            var nullsDouble = new bool[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (rawResults[i] == null) { nullsDouble[i] = true; continue; }
                dataDouble[i] = Convert.ToDouble(rawResults[i]);
            }
            return new DataColumn<double>(name, dataDouble, nullsDouble);
        }
    }
}