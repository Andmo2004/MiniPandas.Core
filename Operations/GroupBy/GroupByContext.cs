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
    ///   FASE 2 — Materialización (Agg / Count / Apply):
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
    /// </summary>
    public class GroupByContext
    {
        private readonly DataFrame _source;
        private readonly string[] _keys;

        // Mapa de clave de grupo → índices de filas
        // La clave es string para soportar claves compuestas y tipos mixtos.
        private readonly Dictionary<string, List<int>> _groups;

        // Separador para claves compuestas. Cambia si tus datos contienen "|".
        public static string KeySeparator { get; set; } = "|";

        // ── Constructor — Fase 1: agrupación ─────────────────────────────────

        internal GroupByContext(DataFrame source, string[] keys)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (keys == null || keys.Length == 0)
                throw new ArgumentException("At least one key column required.", nameof(keys));

            // Validar que todas las claves existen antes de empezar a iterar
            foreach (var key in keys)
                if (!source.ContainsColumn(key))
                    throw new KeyNotFoundException(
                        $"Key column '{key}' not found in DataFrame.");

            _source = source;
            _keys = keys;
            _groups = new Dictionary<string, List<int>>(StringComparer.Ordinal);

            BuildGroups();
        }

        private void BuildGroups()
        {
            // Precargamos las columnas clave para no hacer lookup por nombre en cada fila
            var keyColumns = new BaseColumn[_keys.Length];
            for (int k = 0; k < _keys.Length; k++)
                keyColumns[k] = _source[_keys[k]];

            for (int row = 0; row < _source.RowCount; row++)
            {
                string groupKey = BuildGroupKey(keyColumns, row);

                if (!_groups.TryGetValue(groupKey, out var list))
                {
                    list = new List<int>();
                    _groups[groupKey] = list;
                }
                list.Add(row);
            }
        }

        private static string BuildGroupKey(BaseColumn[] keyColumns, int row)
        {
            // Caso común optimizado: una sola clave, sin concatenación
            if (keyColumns.Length == 1)
                return GetKeyValue(keyColumns[0], row);

            // Claves compuestas: concatenar con separador
            var parts = new string[keyColumns.Length];
            for (int k = 0; k < keyColumns.Length; k++)
                parts[k] = GetKeyValue(keyColumns[k], row);

            return string.Join(KeySeparator, parts);
        }

        private static string GetKeyValue(BaseColumn col, int row)
        {
            // CategoricalColumn: usamos el código entero como clave — evita comparar strings
            if (col is CategoricalColumn cc)
                return cc.GetCode(row).ToString();

            // Resto de columnas: boxing puntual solo para construir la clave
            var val = col.GetBoxed(row);
            return val == null ? "\0null" : val.ToString();   // \0null para distinguir null de "null"
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
                sizes[kvp.Key] = kvp.Value.Count;
            return sizes;
        }

        // ── Fase 2: materialización ───────────────────────────────────────────

        /// <summary>
        /// Aplica funciones de agregación por columna y devuelve un nuevo DataFrame.
        ///
        /// aggregations: diccionario nombreColumna → función a aplicar.
        ///
        /// El DataFrame resultado tiene:
        ///   - Una columna por cada clave de agrupación (con los valores únicos del grupo).
        ///   - Una columna por cada entrada en aggregations.
        ///   - Una fila por grupo.
        ///
        /// Ejemplo:
        ///   df.GroupBy("pais").Agg(new Dictionary&lt;string, AggFunc&gt; {
        ///       { "ventas", AggFunc.Sum },
        ///       { "precio", AggFunc.Mean }
        ///   });
        /// </summary>
        public DataFrame Agg(Dictionary<string, AggFunc> aggregations)
        {
            if (aggregations == null) throw new ArgumentNullException(nameof(aggregations));
            if (aggregations.Count == 0)
                throw new ArgumentException("At least one aggregation required.", nameof(aggregations));

            // Validar columnas de agregación antes de iterar grupos
            foreach (var colName in aggregations.Keys)
                if (!_source.ContainsColumn(colName))
                    throw new KeyNotFoundException(
                        $"Aggregation column '{colName}' not found in DataFrame.");

            int groupCount = _groups.Count;
            var groupList = _groups.ToList();   // orden estable para construir columnas

            // ── Construir columnas de claves ──────────────────────────────────
            // Necesitamos descodificar la clave compuesta de vuelta a valores por columna
            var keyData = new string[_keys.Length][];
            for (int k = 0; k < _keys.Length; k++)
                keyData[k] = new string[groupCount];

            var keyColumns = new BaseColumn[_keys.Length];
            for (int k = 0; k < _keys.Length; k++)
                keyColumns[k] = _source[_keys[k]];

            for (int g = 0; g < groupCount; g++)
            {
                // Tomamos el primer índice del grupo para leer las claves
                int sampleRow = groupList[g].Value[0];
                for (int k = 0; k < _keys.Length; k++)
                    keyData[k][g] = keyColumns[k].IsNull(sampleRow)
                        ? null
                        : DecodeKeyValue(keyColumns[k], sampleRow);
            }

            // ── Construir columnas de agregación ──────────────────────────────
            // Cada columna de resultado es object[] que luego convertiremos al tipo correcto
            var aggResults = new Dictionary<string, object[]>(aggregations.Count);
            foreach (var colName in aggregations.Keys)
                aggResults[colName] = new object[groupCount];

            for (int g = 0; g < groupCount; g++)
            {
                var indices = groupList[g].Value;
                foreach (var kvp in aggregations)
                    aggResults[kvp.Key][g] = Aggregations.Aggregate(
                        _source[kvp.Key], indices, kvp.Value);
            }

            // ── Ensamblar el DataFrame resultado ──────────────────────────────
            var resultColumns = new List<BaseColumn>();

            // Columnas clave: siempre StringColumn (para soportar cualquier tipo original)
            for (int k = 0; k < _keys.Length; k++)
                resultColumns.Add(new StringColumn(_keys[k], keyData[k]));

            // Columnas de agregación: double si es numérico, int si es Count
            foreach (var kvp in aggregations)
            {
                var rawResults = aggResults[kvp.Key];
                resultColumns.Add(BuildAggColumn(kvp.Key, kvp.Value, rawResults, groupCount));
            }

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

            // Columnas clave
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

                counts[g] = groupList[g].Value.Count;
            }

            var resultColumns = new List<BaseColumn>();
            for (int k = 0; k < _keys.Length; k++)
                resultColumns.Add(new StringColumn(_keys[k], keyData[k]));

            resultColumns.Add(new DataColumn<double>("count", counts, nulls));

            return new DataFrame(resultColumns);
        }

        /// <summary>
        /// Aplica una función arbitraria a cada grupo y concatena los resultados.
        /// Equivalente a pandas .groupby().apply().
        ///
        /// La función recibe un DataFrame con las filas del grupo
        /// y debe devolver un DataFrame (puede tener distinto número de filas).
        ///
        /// ATENCIÓN: Apply() es flexible pero lento. Para agregaciones estándar
        /// usa Agg() que es significativamente más eficiente.
        /// </summary>
        public DataFrame Apply(Func<DataFrame, DataFrame> func)
        {
            if (func == null) throw new ArgumentNullException(nameof(func));

            var resultParts = new List<DataFrame>();

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
            var val = col.GetBoxed(row);
            return val?.ToString();
        }

        /// <summary>
        /// Construye la columna de resultado de una agregación.
        /// Count devuelve DataColumn&lt;int&gt;, el resto DataColumn&lt;double&gt;.
        /// Los nulls se preservan cuando el grupo estaba vacío o todo era nulo.
        /// </summary>
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
        /// Concatena una lista de DataFrames verticalmente.
        /// Todos deben tener las mismas columnas y tipos.
        /// Usado internamente por Apply().
        /// </summary>
        private static DataFrame Concat(List<DataFrame> parts)
        {
            // Tomamos el esquema del primero
            var first = parts[0];
            int totalRows = parts.Sum(p => p.RowCount);
            var columnNames = first.ColumnNames.ToList();

            // Para cada columna, recopilamos todos los valores en orden
            var resultColumns = new List<BaseColumn>();

            foreach (var colName in columnNames)
            {
                var allBoxed = new object[totalRows];
                int offset = 0;
                // bool hasNull = false;

                foreach (var part in parts)
                {
                    if (!part.ContainsColumn(colName))
                        throw new InvalidOperationException(
                            $"Apply result is missing column '{colName}'.");

                    var col = part[colName];
                    for (int r = 0; r < part.RowCount; r++)
                    {
                        allBoxed[offset] = col.GetBoxed(r);
                        // if (allBoxed[offset] == null) hasNull = true;
                        offset++;
                    }
                }

                resultColumns.Add(BuildColumnFromBoxed(colName, allBoxed, first[colName]));
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
            {
                var data = new string[boxed.Length];
                for (int i = 0; i < boxed.Length; i++)
                    data[i] = boxed[i]?.ToString();
                return new StringColumn(name, data);
            }
        }
    }
}