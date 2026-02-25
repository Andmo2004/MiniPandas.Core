using System;
using System.Collections.Generic;
using System.Linq;
using MiniPandas.Core.Columns;
using MiniPandas.Core.Operations.GroupBy;
using MiniPandas.Core.Operations.Merge;

namespace MiniPandas.Core
{
    public class DataFrame
    {
        private readonly Dictionary<string, BaseColumn> _columns;

        // Orden de inserción: crítico para Head(), iteración, display
        private readonly List<string> _columnOrder;

        public int RowCount { get; private set; }
        public int ColumnCount => _columns.Count;

        // Nombres en orden de inserción, como pandas .columns
        public IEnumerable<string> ColumnNames => _columnOrder;
        public IEnumerable<BaseColumn> Columns => _columnOrder.Select(n => _columns[n]);

        // ── Constructores ─────────────────────────────────────────────────────

        /// <summary>
        /// Constructor público para uso externo.
        /// El usuario indica cuántas filas tendrá el DataFrame antes de añadir columnas.
        /// Todas las columnas añadidas deben tener exactamente esa cantidad de filas.
        /// </summary>
        public DataFrame(int rows)
        {
            if (rows < 0) throw new ArgumentOutOfRangeException(nameof(rows));
            RowCount = rows;
            _columns = new Dictionary<string, BaseColumn>(StringComparer.OrdinalIgnoreCase);
            _columnOrder = new List<string>();
        }

        /// <summary>
        /// Constructor interno para operaciones que producen nuevos DataFrames
        /// (Filter, Merge, GroupBy, Select columnas, etc.).
        /// El RowCount se deduce de la primera columna; no es necesario conocerlo de antemano.
        /// Las columnas deben tener todas la misma longitud.
        /// </summary>
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

            // DataFrame vacío (sin columnas) es válido: RowCount = 0
            RowCount = expectedRows ?? 0;
        }

        // ── Gestión de columnas ───────────────────────────────────────────────

        public void AddColumn(BaseColumn column)
        {
            if (column == null) throw new ArgumentNullException(nameof(column));
            if (column.Length != RowCount)
                throw new ArgumentException(
                    $"Column '{column.Name}' has {column.Length} rows, expected {RowCount}.");

            if (!_columns.ContainsKey(column.Name))
                _columnOrder.Add(column.Name);   // nueva columna: registrar orden

            _columns[column.Name] = column;      // reemplazar si ya existe (como pandas)
        }

        public bool TryRemoveColumn(string name)
        {
            if (!_columns.Remove(name)) return false;
            _columnOrder.Remove(name);
            return true;
        }

        public bool ContainsColumn(string name) => _columns.ContainsKey(name);

        // ── Indexadores ───────────────────────────────────────────────────────

        /// <summary>df["precio"] → columna completa.</summary>
        public BaseColumn this[string columnName]
        {
            get
            {
                if (!_columns.TryGetValue(columnName, out var col))
                    throw new KeyNotFoundException($"Column '{columnName}' not found.");
                return col;
            }
        }

        /// <summary>df[2] → tercera columna (equivalente a df.iloc[:, 2]).</summary>
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

        /// <summary>
        /// Acceso tipado fuerte: df.GetColumn&lt;double&gt;("precio").
        /// Lanza InvalidCastException si el tipo no coincide.
        /// </summary>
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

        /// <summary>
        /// Filtra filas devolviendo un nuevo DataFrame inmutable.
        /// mask[i] == true → la fila i se incluye en el resultado.
        /// Equivalente a pandas: df[mask]
        /// </summary>
        public DataFrame Where(bool[] mask)
        {
            if (mask == null) throw new ArgumentNullException(nameof(mask));
            if (mask.Length != RowCount)
                throw new ArgumentException(
                    $"Mask length ({mask.Length}) must match RowCount ({RowCount}).", nameof(mask));

            // Cada columna sabe filtrarse a sí misma — no hay acoplamiento de tipos aquí
            var filteredColumns = Columns.Select(col => col.Filter(mask));
            return new DataFrame(filteredColumns);
        }

        /// <summary>
        /// Devuelve un nuevo DataFrame con las primeras <paramref name="n"/> filas.
        /// Si n &gt;= RowCount devuelve todas las filas (no lanza excepción, como pandas).
        /// Equivalente a pandas: df.head(n)
        /// </summary>
        /// <param name="n">Número de filas a devolver. Debe ser mayor que cero.</param>
        public DataFrame Head(int n = 5)
        {
            if (n <= 0)
                throw new ArgumentOutOfRangeException(nameof(n),
                    "n must be greater than zero.");

            // Clamp: si n supera el total de filas devolvemos todo, como pandas
            int take = System.Math.Min(n, RowCount);

            var mask = new bool[RowCount];
            for (int i = 0; i < take; i++)
                mask[i] = true;

            return Where(mask);
        }

        /// <summary>
        /// Devuelve un nuevo DataFrame con las últimas <paramref name="n"/> filas.
        /// Si n &gt;= RowCount devuelve todas las filas (no lanza excepción, como pandas).
        /// Equivalente a pandas: df.tail(n)
        /// </summary>
        /// <param name="n">Número de filas a devolver. Debe ser mayor que cero.</param>
        public DataFrame Tail(int n = 5)
        {
            if (n <= 0)
                throw new ArgumentOutOfRangeException(nameof(n),
                    "n must be greater than zero.");

            // Clamp: si n supera el total de filas devolvemos todo, como pandas
            int take = System.Math.Min(n, RowCount);
            int start = RowCount - take;

            var mask = new bool[RowCount];
            for (int i = start; i < RowCount; i++)
                mask[i] = true;

            return Where(mask);
        }

        /// <summary>
        /// Une este DataFrame con <paramref name="right"/> por una columna clave
        /// con el mismo nombre en ambos.
        /// Equivalente a pandas: df.merge(right, on="columna", how=...)
        /// </summary>
        /// <param name="right">DataFrame derecho.</param>
        /// <param name="on">Nombre de la columna clave, igual en ambos lados.</param>
        /// <param name="how">Tipo de join. Por defecto Inner.</param>
        public DataFrame Merge(DataFrame right, string on, JoinType how = JoinType.Inner)
        {
            if (on == null) throw new ArgumentNullException(nameof(on));
            return Merge(right, new[] { on }, new[] { on }, how);
        }

        /// <summary>
        /// Une este DataFrame con <paramref name="right"/> por varias columnas clave
        /// con el mismo nombre en ambos.
        /// Equivalente a pandas: df.merge(right, on=["col1","col2"], how=...)
        /// </summary>
        /// <param name="right">DataFrame derecho.</param>
        /// <param name="on">Nombres de las columnas clave, iguales en ambos lados.</param>
        /// <param name="how">Tipo de join. Por defecto Inner.</param>
        public DataFrame Merge(DataFrame right, string[] on, JoinType how = JoinType.Inner)
        {
            if (on == null) throw new ArgumentNullException(nameof(on));
            return Merge(right, on, on, how);
        }

        /// <summary>
        /// Une este DataFrame con <paramref name="right"/> permitiendo nombres de clave
        /// distintos en cada lado.
        /// Equivalente a pandas: df.merge(right, left_on=[...], right_on=[...], how=...)
        ///
        /// Las columnas clave aparecen una sola vez en el resultado con el nombre de la
        /// clave izquierda. Las columnas no-clave con nombre duplicado en ambos lados
        /// reciben sufijo "_x" (izquierda) y "_y" (derecha).
        /// </summary>
        /// <param name="right">DataFrame derecho.</param>
        /// <param name="leftOn">Columnas clave del DataFrame izquierdo.</param>
        /// <param name="rightOn">Columnas clave del DataFrame derecho. Mismo número que leftOn.</param>
        /// <param name="how">Tipo de join. Por defecto Inner.</param>
        public DataFrame Merge(DataFrame right, string[] leftOn, string[] rightOn, JoinType how = JoinType.Inner)
        {
            if (right == null) throw new ArgumentNullException(nameof(right));
            if (leftOn == null) throw new ArgumentNullException(nameof(leftOn));
            if (rightOn == null) throw new ArgumentNullException(nameof(rightOn));
            if (leftOn.Length != rightOn.Length)
                throw new ArgumentException(
                    $"leftOn y rightOn deben tener la misma longitud " +
                    $"({leftOn.Length} vs {rightOn.Length}).");

            return MergeOp.Execute(this, right, leftOn, rightOn, how);
        }

        /// <summary>
        /// Agrupa el DataFrame por una o más columnas.
        /// Devuelve un GroupByContext sobre el que llamar .Agg(), .Count() o .Apply().
        /// Equivalente a pandas: df.groupby(["col1", "col2"])
        /// </summary>
        public GroupByContext GroupBy(params string[] keys)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentException("At least one key column required.", nameof(keys));
            return new GroupByContext(this, keys);
        }

        /// <summary>
        /// Devuelve un nuevo DataFrame con solo las columnas indicadas, en el orden dado.
        /// Equivalente a pandas: df[["col1", "col2"]]
        /// </summary>
        public DataFrame Select(params string[] columnNames)
        {
            if (columnNames == null) throw new ArgumentNullException(nameof(columnNames));
            if (columnNames.Length == 0)
                throw new ArgumentException("At least one column name must be specified.");

            var selected = columnNames.Select(name => this[name]);   // lanza si no existe
            return new DataFrame(selected);
        }
    }
}