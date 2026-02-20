using System;
using System.Collections.Generic;
using System.Linq;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core
{
    public class DataFrame
    {
        private readonly Dictionary<string, BaseColumn> _columns;

        // Orden de inserción: crítico para Head(), iteración, display
        private readonly List<string> _columnOrder;

        public int RowCount { get; }
        public int ColumnCount => _columns.Count;

        // Nombres en orden de inserción, como pandas .columns
        public IEnumerable<string> ColumnNames => _columnOrder;
        public IEnumerable<BaseColumn> Columns => _columnOrder.Select(n => _columns[n]);

        public DataFrame(int rows)
        {
            if (rows < 0) throw new ArgumentOutOfRangeException(nameof(rows));
            RowCount = rows;
            _columns = new Dictionary<string, BaseColumn>(StringComparer.OrdinalIgnoreCase);
            _columnOrder = new List<string>();
        }

        public void AddColumn(BaseColumn column)
        {
            if (column == null) throw new ArgumentNullException(nameof(column));
            if (column.Length != RowCount)
                throw new ArgumentException(
                    $"Column '{column.Name}' has {column.Length} rows, expected {RowCount}.");

            if (!_columns.ContainsKey(column.Name))
                _columnOrder.Add(column.Name); // nueva columna: registrar orden

            _columns[column.Name] = column;    // reemplazar si ya existe (como pandas)
        }

        public bool TryRemoveColumn(string name)
        {
            if (!_columns.Remove(name)) return false;
            _columnOrder.Remove(name);
            return true;
        }

        // Indexador por nombre
        public BaseColumn this[string columnName]
        {
            get
            {
                if (!_columns.TryGetValue(columnName, out var col))
                    throw new KeyNotFoundException($"Column '{columnName}' not found.");
                return col;
            }
        }

        // Indexador posicional, como df.iloc[:,i]
        public BaseColumn this[int index]
        {
            get
            {
                if ((uint)index >= (uint)_columnOrder.Count)
                    throw new ArgumentOutOfRangeException(nameof(index));
                return _columns[_columnOrder[index]];
            }
        }

        public bool ContainsColumn(string name) => _columns.ContainsKey(name);

        // Tipado fuerte: df.GetColumn<double>("precio")
        public DataColumn<T> GetColumn<T>(string name) where T : struct
        {
            var col = this[name];
            return col as DataColumn<T>
                ?? throw new InvalidCastException(
                    $"Column '{name}' is not DataColumn<{typeof(T).Name}>.");
        }

        public StringColumn GetStringColumn(string name)
        {
            var col = this[name];
            return col as StringColumn
                ?? throw new InvalidCastException($"Column '{name}' is not a StringColumn.");
        }
    }
}