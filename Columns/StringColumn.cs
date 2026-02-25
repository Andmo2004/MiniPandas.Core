// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace MiniPandas.Core.Columns
{
    /// <summary>
    /// Columna especializada para strings.
    /// No usa DataColumn&lt;T&gt; porque string es un tipo referencia:
    ///   - null en el array ya tiene semántica de celda nula, sin necesitar BitArray.
    ///   - No es posible usar T? (Nullable&lt;T&gt;) con tipos referencia en .NET 4.7.2.
    ///   - Es intercambiable con DataColumn&lt;T&gt; vía BaseColumn (GetBoxed, Filter).
    /// </summary>
    public class StringColumn : BaseColumn, IEnumerable<string>
    {
        private readonly string[] _data;

        public override int Length => _data.Length;

        // ── Constructores ─────────────────────────────────────────────────────

        public StringColumn(string name, int rows) : base(name)
        {
            if (rows < 0) throw new ArgumentOutOfRangeException(nameof(rows));
            _data = new string[rows];   // inicializa a null: toda celda empieza nula
        }

        public StringColumn(string name, string[] data) : base(name)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            _data = (string[])data.Clone();   // defensivo: no compartir referencia
        }

        // ── Indexer ───────────────────────────────────────────────────────────

        /// <summary>
        /// Devuelve null si la celda es nula (consistente con pandas NaN para strings).
        /// Asignar null marca la celda como nula. string.Empty NO es nulo (igual que pandas).
        /// </summary>
        public string this[int index]
        {
            get
            {
                ValidateIndex(index);
                return _data[index];
            }
            set
            {
                ValidateIndex(index);
                _data[index] = value;
            }
        }

        // ── Implementación de BaseColumn ──────────────────────────────────────

        public override bool IsNull(int index)
        {
            ValidateIndex(index);
            return _data[index] is null;
        }

        /// <inheritdoc/>
        /// Para StringColumn el boxing es trivial: string ya es un tipo referencia.
        public override object GetBoxed(int index)
        {
            ValidateIndex(index);
            return _data[index];   // null si nulo, string si no — sin conversión
        }

        /// <inheritdoc/>
        /// Produce una nueva StringColumn con solo las filas donde mask[i] == true.
        public override BaseColumn Filter(bool[] mask)
        {
            ValidateMask(mask);

            var result = new List<string>(capacity: mask.Length);
            for (int i = 0; i < mask.Length; i++)
            {
                if (mask[i]) result.Add(_data[i]);   // null se preserva tal cual
            }

            return new StringColumn(Name, result.ToArray());
        }

        // ── Acceso raw para operaciones vectorizadas ──────────────────────────

        /// <summary>
        /// Vista de solo lectura del array interno.
        /// Útil para búsquedas y filtros sin overhead de validación.
        /// </summary>
        public ReadOnlySpan<string> AsSpan() => _data.AsSpan();

        // ── IEnumerable<string> ───────────────────────────────────────────────

        public IEnumerator<string> GetEnumerator()
            => ((IEnumerable<string>)_data).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        // ── Helpers privados ──────────────────────────────────────────────────

        private void ValidateIndex(int index)
        {
            if ((uint)index >= (uint)_data.Length)
                throw new ArgumentOutOfRangeException(nameof(index),
                    $"Index {index} is out of range for column '{Name}' (length {_data.Length}).");
        }

        /// <summary>
        /// Devuelve una máscara booleana donde true = celda igual al valor dado.
        /// </summary>
        public bool[] EqualsMask(string value)
        {
            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                mask[i] = string.Equals(_data[i], value, StringComparison.Ordinal);
            return mask;
        }

        /// <summary>
        /// Versión multi-valor: equivalente a pandas isin().
        /// df.Where(col.IsInMask("Madrid", "Barcelona", "Valencia"))
        /// </summary>
        public bool[] IsInMask(params string[] values)
        {
            var mask = new bool[_data.Length];
            if (values == null || values.Length == 0) return mask;
            var set = new HashSet<string>(values, StringComparer.Ordinal);
            for (int i = 0; i < _data.Length; i++)
                mask[i] = _data[i] != null && set.Contains(_data[i]);
            return mask;
        }
    }
}