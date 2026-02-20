using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MiniPandas.Core.Columns
{
    public class DataColumn<T> : BaseColumn, IEnumerable<T?>
        where T : struct                                        // Ver nota sobre strings al final
    {
        private readonly T[] _data;
        private readonly BitArray _nullMask;                    // true = es nulo

        public override int Length => _data.Length;

        public DataColumn(string name, int rows) : base(name)
        {
            if (rows < 0)
                throw new ArgumentOutOfRangeException(nameof(rows));

            _data = new T[rows];
            _nullMask = new BitArray(rows, false);
        }

        // Constructor desde datos existentes
        public DataColumn(string name, T[] data, bool[] nulls = null) : base(name)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            if (nulls != null && nulls.Length != data.Length)
                throw new ArgumentException("nulls length must match data length.");

            _data = (T[])data.Clone();                                 // Defensivo: no compartir referencia
            _nullMask = nulls != null
                ? new BitArray(nulls)
                : new BitArray(data.Length, false);
        }

        // Indexador con semántica nullable (como pandas: columna[i] puede ser None)
        public T? this[int index]
        {
            get
            {
                ValidateIndex(index);
                return _nullMask[index] ? (T?)null : _data[index];
            }
            set
            {
                ValidateIndex(index);
                if (value.HasValue)
                {
                    _data[index] = value.Value;
                    _nullMask[index] = false;
                }
                else
                {
                    _data[index] = default;
                    _nullMask[index] = true;
                }
            }
        }

        public override bool IsNull(int index)
        {
            ValidateIndex(index);
            return _nullMask[index];
        }

        // Acceso al valor crudo sin boxing, útil internamente para operaciones vectorizadas
        public T GetRawValue(int index) => _data[index];

        // Expone una vista de solo lectura del array para operaciones vectorizadas (LINQ, SIMD futuro)
        public ReadOnlySpan<T> AsSpan() => _data.AsSpan();

        public IEnumerator<T?> GetEnumerator()
        {
            for (int i = 0; i < _data.Length; i++)
                yield return _nullMask[i] ? (T?)null : _data[i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        private void ValidateIndex(int index)
        {
            if ((uint)index >= (uint)_data.Length)
                throw new ArgumentOutOfRangeException(nameof(index));
        }
    }
}
