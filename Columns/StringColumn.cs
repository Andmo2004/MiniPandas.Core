// ── StringColumn ────────────────────────────────────────────────────────────

using System;
using System.Collections;
using System.Collections.Generic;

namespace MiniPandas.Core.Columns
{
    public class StringColumn : BaseColumn, IEnumerable<string>
    {
        private readonly string[] _data;

        public override int Length => _data.Length;

        public StringColumn(string name, int rows) : base(name)
        {
            if (rows < 0) throw new ArgumentOutOfRangeException(nameof(rows));
            _data = new string[rows];
        }

        // Constructor desde datos existentes, consistente con DataColumn<T>
        public StringColumn(string name, string[] data) : base(name)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            _data = (string[])data.Clone();
        }

        public string this[int index]
        {
            get
            {
                ValidateIndex(index);
                return _data[index]; // null es válido: significa celda nula
            }
            set
            {
                ValidateIndex(index);
                // Tratar string.Empty como nulo es decisión de diseño:
                // pandas NO lo hace, así que aquí tampoco.
                _data[index] = value;
            }
        }

        public override bool IsNull(int index)
        {
            ValidateIndex(index);
            return _data[index] is null;
        }

        // Útil para búsquedas/filtros sin nullable overhead
        public ReadOnlySpan<string> AsSpan() => _data.AsSpan();

        public IEnumerator<string> GetEnumerator()
            => ((IEnumerable<string>)_data).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        private void ValidateIndex(int index)
        {
            if ((uint)index >= (uint)_data.Length)
                throw new ArgumentOutOfRangeException(nameof(index));
        }
    }
}