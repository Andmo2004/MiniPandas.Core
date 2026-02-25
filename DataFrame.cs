using System;
using System.Collections.Generic;
using System.Linq;
using MiniPandas.Core.Columns;
using MiniPandas.Core.Operations;
using MiniPandas.Core.Operations.GroupBy;
using MiniPandas.Core.Operations.Merge;

namespace MiniPandas.Core
{
    public class DataFrame
    {
        private readonly Dictionary<string, BaseColumn> _columns;
        private readonly List<string> _columnOrder;

        public int RowCount { get; private set; }
        public int ColumnCount => _columns.Count;

        public IEnumerable<string> ColumnNames => _columnOrder;
        public IEnumerable<BaseColumn> Columns => _columnOrder.Select(n => _columns[n]);

        // ── Constructores ─────────────────────────────────────────────────────

        public DataFrame(int rows)
        {
            if (rows < 0) throw new ArgumentOutOfRangeException(nameof(rows));
            RowCount = rows;
            _columns = new Dictionary<string, BaseColumn>(StringComparer.OrdinalIgnoreCase);
            _columnOrder = new List<string>();
        }

        internal DataFrame(IEnumerable<BaseColumn> columns)
        {
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            _columns = new Dictionary<string, BaseColumn>(StringComparer.OrdinalIgnoreCase);
            _columnOrder = new List<string>();

            int? expectedRows = null;

            foreach (var col in columns)
            {
                if (col == null)
                    throw new ArgumentException("Column list must not contain null entries.");

                if (expectedRows == null)
                    expectedRows = col.Length;
                else if (col.Length != expectedRows.Value)
                    throw new ArgumentException(
                        $"Column '{col.Name}' has {col.Length} rows but expected {expectedRows.Value}.");

                if (!_columns.ContainsKey(col.Name))
                    _columnOrder.Add(col.Name);

                _columns[col.Name] = col;
            }

            RowCount = expectedRows ?? 0;
        }

        // ── Factoría pública ──────────────────────────────────────────────────

        /// <summary>
        /// Crea un DataFrame a partir de una colección de columnas ya construidas.
        /// Todas las columnas deben tener la misma longitud.
        ///
        ///   var df = DataFrame.FromColumns(
        ///       new DataColumn&lt;double&gt;("precio", prices),
        ///       new StringColumn("ciudad", cities));
        /// </summary>
        public static DataFrame FromColumns(IEnumerable<BaseColumn> columns)
        {
            if (columns == null) throw new ArgumentNullException(nameof(columns));
            return new DataFrame(columns);
        }

        /// <summary>
        /// Sobrecarga con params: permite pasar columnas directamente sin crear una lista.
        ///
        ///   var df = DataFrame.FromColumns(colPrecio, colCiudad, colFecha);
        /// </summary>
        public static DataFrame FromColumns(params BaseColumn[] columns)
        {
            if (columns == null) throw new ArgumentNullException(nameof(columns));
            return new DataFrame(columns);
        }

        // ── Gestión de columnas ───────────────────────────────────────────────

        public void AddColumn(BaseColumn column)
        {
            if (column == null) throw new ArgumentNullException(nameof(column));
            if (column.Length != RowCount)
                throw new ArgumentException(
                    $"Column '{column.Name}' has {column.Length} rows, expected {RowCount}.");

            if (!_columns.ContainsKey(column.Name))
                _columnOrder.Add(column.Name);

            _columns[column.Name] = column;
        }

        public bool TryRemoveColumn(string name)
        {
            if (!_columns.Remove(name)) return false;
            _columnOrder.Remove(name);
            return true;
        }

        public bool ContainsColumn(string name) => _columns.ContainsKey(name);

        // ── Indexadores ───────────────────────────────────────────────────────

        public BaseColumn this[string columnName]
        {
            get
            {
                if (!_columns.TryGetValue(columnName, out var col))
                    throw new KeyNotFoundException($"Column '{columnName}' not found.");
                return col;
            }
        }

        public BaseColumn this[int index]
        {
            get
            {
                if ((uint)index >= (uint)_columnOrder.Count)
                    throw new ArgumentOutOfRangeException(nameof(index));
                return _columns[_columnOrder[index]];
            }
        }

        // ── Acceso tipado ─────────────────────────────────────────────────────

        public DataColumn<T> GetColumn<T>(string name) where T : struct, IComparable<T>
        {
            var col = this[name];
            return col as DataColumn<T>
                ?? throw new InvalidCastException(
                    $"Column '{name}' is not DataColumn<{typeof(T).Name}>. Actual type: {col.GetType().Name}.");
        }

        public StringColumn GetStringColumn(string name)
        {
            var col = this[name];
            return col as StringColumn
                ?? throw new InvalidCastException(
                    $"Column '{name}' is not a StringColumn. Actual type: {col.GetType().Name}.");
        }

        // ── Operaciones que producen nuevos DataFrames ────────────────────────

        public DataFrame Where(bool[] mask)
        {
            if (mask == null) throw new ArgumentNullException(nameof(mask));
            if (mask.Length != RowCount)
                throw new ArgumentException(
                    $"Mask length ({mask.Length}) must match RowCount ({RowCount}).", nameof(mask));

            var filteredColumns = Columns.Select(col => col.Filter(mask));
            return new DataFrame(filteredColumns);
        }

        /// <summary>
        /// Devuelve un nuevo DataFrame con solo las filas indicadas por <paramref name="indices"/>.
        ///
        /// A DIFERENCIA de Where(bool[]):
        ///   - Los índices no tienen que ser contiguos ni estar ordenados.
        ///   - Permite repetir filas (índice duplicado → fila duplicada en el resultado).
        ///   - Es O(k) donde k = indices.Length, no O(n) sobre el DataFrame completo.
        ///
        /// Usado internamente por GroupByContext para materializar grupos directamente
        /// desde sus índices, sin construir una máscara booleana del tamaño total.
        ///
        /// indices[i] == -1 → celda nula (coherente con la semántica de MergeOp).
        /// </summary>
        internal DataFrame GatherRows(int[] indices)
        {
            if (indices == null) throw new ArgumentNullException(nameof(indices));

            var gatheredColumns = _columnOrder
                .Select(name => ColumnGather.Gather(_columns[name], indices));

            return new DataFrame(gatheredColumns);
        }

        public DataFrame Head(int n = 5)
        {
            if (n <= 0) throw new ArgumentOutOfRangeException(nameof(n), "n must be greater than zero.");

            int take = System.Math.Min(n, RowCount);
            var mask = new bool[RowCount];
            for (int i = 0; i < take; i++) mask[i] = true;
            return Where(mask);
        }

        public DataFrame Tail(int n = 5)
        {
            if (n <= 0) throw new ArgumentOutOfRangeException(nameof(n), "n must be greater than zero.");

            int take = System.Math.Min(n, RowCount);
            int start = RowCount - take;
            var mask = new bool[RowCount];
            for (int i = start; i < RowCount; i++) mask[i] = true;
            return Where(mask);
        }

        public DataFrame Merge(DataFrame right, string on, JoinType how = JoinType.Inner)
        {
            if (on == null) throw new ArgumentNullException(nameof(on));
            return Merge(right, new[] { on }, new[] { on }, how);
        }

        public DataFrame Merge(DataFrame right, string[] on, JoinType how = JoinType.Inner)
        {
            if (on == null) throw new ArgumentNullException(nameof(on));
            return Merge(right, on, on, how);
        }

        public DataFrame Merge(DataFrame right, string[] leftOn, string[] rightOn, JoinType how = JoinType.Inner)
        {
            if (right == null) throw new ArgumentNullException(nameof(right));
            if (leftOn == null) throw new ArgumentNullException(nameof(leftOn));
            if (rightOn == null) throw new ArgumentNullException(nameof(rightOn));
            if (leftOn.Length != rightOn.Length)
                throw new ArgumentException(
                    $"leftOn y rightOn deben tener la misma longitud ({leftOn.Length} vs {rightOn.Length}).");

            return MergeOp.Execute(this, right, leftOn, rightOn, how);
        }

        /// <summary>
        /// Agrupa el DataFrame por una o más columnas.
        /// Equivalente a pandas: df.groupby(["col1", "col2"])
        /// </summary>
        public GroupByContext GroupBy(params string[] keys)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentException("At least one key column required.", nameof(keys));
            return new GroupByContext(this, keys);
        }

        /// <summary>
        /// Agrupa el DataFrame con opciones de agrupación explícitas.
        /// Útil cuando los datos pueden contener el separador de clave por defecto ("|").
        ///
        ///   var opts = new GroupByOptions(keySeparator: "\x00|\x00");
        ///   df.GroupBy(opts, "pais", "ciudad")
        /// </summary>
        public GroupByContext GroupBy(GroupByOptions options, params string[] keys)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentException("At least one key column required.", nameof(keys));
            return new GroupByContext(this, keys, options);
        }

        public DataFrame Select(params string[] columnNames)
        {
            if (columnNames == null) throw new ArgumentNullException(nameof(columnNames));
            if (columnNames.Length == 0)
                throw new ArgumentException("At least one column name must be specified.");

            var selected = columnNames.Select(name => this[name]);
            return new DataFrame(selected);
        }
    }
}