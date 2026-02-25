// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace MiniPandas.Core.Columns
{
    /// <summary>
    /// Columna tipada para tipos valor (double, int, DateTime, bool, etc.).
    /// La restricción "where T : struct" es deliberada y correcta:
    ///   - Permite usar T? (Nullable&lt;T&gt;) en el indexer con semántica null clara.
    ///   - Para strings existe StringColumn, que maneja null de forma nativa.
    ///   - Ambas clases implementan BaseColumn y son intercambiables vía GetBoxed/Filter.
    ///
    /// La restricción adicional IComparable&lt;T&gt; habilita los métodos de comparación
    /// (GreaterThan, LessThan, etc.) sin necesidad de conocer el tipo concreto.
    /// </summary>
    public class DataColumn<T> : BaseColumn, IEnumerable<T?>
        where T : struct, IComparable<T>
    {
        private readonly T[] _data;
        private readonly BitArray _nullMask;   // true = celda nula

        public override int Length => _data.Length;

        // ── Constructores ─────────────────────────────────────────────────────

        public DataColumn(string name, int rows) : base(name)
        {
            if (rows < 0)
                throw new ArgumentOutOfRangeException(nameof(rows));

            _data = new T[rows];
            _nullMask = new BitArray(rows, false);
        }

        public DataColumn(string name, T[] data, bool[] nulls = null) : base(name)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            if (nulls != null && nulls.Length != data.Length)
                throw new ArgumentException("nulls length must match data length.", nameof(nulls));

            _data = (T[])data.Clone();    // defensivo: no compartir referencia
            _nullMask = nulls != null
                ? new BitArray(nulls)
                : new BitArray(data.Length, false);
        }

        // ── Indexer con semántica nullable ────────────────────────────────────

        /// <summary>
        /// Acceso con semántica pandas: devuelve null si la celda es nula.
        /// Para acceso sin overhead nullable en hot-paths usa GetRawValue.
        /// </summary>
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

        // ── Implementación de BaseColumn ──────────────────────────────────────

        public override bool IsNull(int index)
        {
            ValidateIndex(index);
            return _nullMask[index];
        }

        /// <inheritdoc/>
        /// Boxing controlado: solo para display, JSON y comparaciones heterogéneas.
        public override object GetBoxed(int index)
        {
            ValidateIndex(index);
            return _nullMask[index] ? null : (object)_data[index];
        }

        /// <inheritdoc/>
        /// Produce una nueva DataColumn&lt;T&gt; con solo las filas donde mask[i] == true.
        public override BaseColumn Filter(bool[] mask)
        {
            ValidateMask(mask);

            var resultData = new System.Collections.Generic.List<T>(capacity: mask.Length);
            var resultNulls = new System.Collections.Generic.List<bool>(capacity: mask.Length);

            for (int i = 0; i < mask.Length; i++)
            {
                if (mask[i])
                {
                    resultData.Add(_data[i]);
                    resultNulls.Add(_nullMask[i]);
                }
            }

            return new DataColumn<T>(Name, resultData.ToArray(), resultNulls.ToArray());
        }

        // ── Acceso raw para operaciones vectorizadas ──────────────────────────

        /// <summary>
        /// Devuelve el valor subyacente sin comprobación de null.
        /// Úsalo en hot-paths tras verificar IsNull manualmente.
        /// </summary>
        public T GetRawValue(int index) => _data[index];

        /// <summary>
        /// Vista de solo lectura del array interno.
        /// Útil para operaciones vectorizadas.
        /// </summary>
        public ReadOnlySpan<T> AsSpan() => _data.AsSpan();

        // ── Comparaciones — devuelven bool[] para usar con DataFrame.Where ────
        //
        // Las celdas nulas siempre producen false en la máscara.
        // Esto es consistente con pandas: NaN comparado con cualquier cosa es False.

        /// <summary>
        /// mask[i] = true si col[i] &gt; value (y no es null).
        /// </summary>
        public bool[] GreaterThan(T value)
        {
            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                mask[i] = !_nullMask[i] && _data[i].CompareTo(value) > 0;
            return mask;
        }

        /// <summary>
        /// mask[i] = true si col[i] &gt;= value (y no es null).
        /// </summary>
        public bool[] GreaterThanOrEqual(T value)
        {
            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                mask[i] = !_nullMask[i] && _data[i].CompareTo(value) >= 0;
            return mask;
        }

        /// <summary>
        /// mask[i] = true si col[i] &lt; value (y no es null).
        /// </summary>
        public bool[] LessThan(T value)
        {
            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                mask[i] = !_nullMask[i] && _data[i].CompareTo(value) < 0;
            return mask;
        }

        /// <summary>
        /// mask[i] = true si col[i] &lt;= value (y no es null).
        /// </summary>
        public bool[] LessThanOrEqual(T value)
        {
            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                mask[i] = !_nullMask[i] && _data[i].CompareTo(value) <= 0;
            return mask;
        }

        /// <summary>
        /// mask[i] = true si col[i] == value (y no es null).
        /// </summary>
        public bool[] EqualTo(T value)
        {
            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                mask[i] = !_nullMask[i] && _data[i].CompareTo(value) == 0;
            return mask;
        }

        /// <summary>
        /// mask[i] = true si col[i] != value (y no es null).
        /// Las celdas nulas devuelven false (no sabemos si son distintas).
        /// </summary>
        public bool[] NotEqualTo(T value)
        {
            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                mask[i] = !_nullMask[i] && _data[i].CompareTo(value) != 0;
            return mask;
        }

        /// <summary>
        /// mask[i] = true si col[i] es null.
        /// Equivalente a pandas: col.isna()
        /// </summary>
        public bool[] IsNullMask()
        {
            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                mask[i] = _nullMask[i];
            return mask;
        }

        /// <summary>
        /// mask[i] = true si col[i] NO es null.
        /// Equivalente a pandas: col.notna()
        /// </summary>
        public bool[] IsNotNullMask()
        {
            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                mask[i] = !_nullMask[i];
            return mask;
        }

        /// <summary>
        /// mask[i] = true si col[i] está entre lower y upper (ambos inclusive).
        /// Equivalente a pandas: col.between(lower, upper).
        /// Las celdas nulas devuelven false.
        /// </summary>
        public bool[] Between(T lower, T upper)
        {
            if (lower.CompareTo(upper) > 0)
                throw new ArgumentException(
                    $"lower ({lower}) must be <= upper ({upper}).");

            var mask = new bool[_data.Length];
            for (int i = 0; i < _data.Length; i++)
            {
                if (_nullMask[i]) continue;
                mask[i] = _data[i].CompareTo(lower) >= 0
                       && _data[i].CompareTo(upper) <= 0;
            }
            return mask;
        }

        // ── IEnumerable<T?> ───────────────────────────────────────────────────

        public IEnumerator<T?> GetEnumerator()
        {
            for (int i = 0; i < _data.Length; i++)
                yield return _nullMask[i] ? (T?)null : _data[i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        // ── Helper privado ────────────────────────────────────────────────────

        private void ValidateIndex(int index)
        {
            if ((uint)index >= (uint)_data.Length)
                throw new ArgumentOutOfRangeException(nameof(index),
                    $"Index {index} is out of range for column '{Name}' (length {_data.Length}).");
        }
    }
}