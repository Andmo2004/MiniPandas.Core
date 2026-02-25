// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace MiniPandas.Core.Columns
{
    /// <summary>
    /// Columna categórica con codificación por diccionario (dictionary encoding).
    ///
    /// CÓMO FUNCIONA:
    ///   En lugar de guardar el string en cada fila, guarda un int (el código).
    ///   El diccionario traduce código → string y string → código.
    ///
    ///   Ejemplo con 1.000.000 filas y 5 categorías distintas:
    ///     StringColumn      → 1.000.000 referencias a string  (~8 MB solo en punteros)
    ///     CategoricalColumn → 1.000.000 ints de 4 bytes       (~4 MB) + 5 strings (~negligible)
    ///
    ///   La ganancia real viene de la deduplicación: si "Madrid" aparece 200.000 veces,
    ///   solo existe UNA vez en memoria, referenciada por 200.000 ints.
    ///
    /// CÓDIGO ESPECIAL:
    ///   -1 = celda nula (equivalente al NaN de pandas para categoricals).
    ///   Los códigos válidos empiezan en 0.
    ///
    /// INMUTABILIDAD DE CATEGORÍAS:
    ///   Las categorías son fijas tras la construcción (como pandas Categorical por defecto).
    ///   El indexer es de solo lectura desde fuera del ensamblado.
    ///   El setter es internal: solo operaciones internas pueden reasignar celdas,
    ///   y solo a valores que ya sean categorías conocidas.
    ///   Para añadir categorías nuevas o modificar desde fuera, usa ToStringColumn(),
    ///   modifica el array y reconstruye con new CategoricalColumn(name, data).
    /// </summary>
    public class CategoricalColumn : BaseColumn, IEnumerable<string>
    {
        // ── Almacenamiento interno ────────────────────────────────────────────

        private readonly int[] _codes;                              // índice en _categories, -1 = nulo
        private readonly List<string> _categories;                  // código → string
        private readonly Dictionary<string, int> _categoryToCode;   // string → código (O(1) lookup)

        // ── Propiedades públicas ──────────────────────────────────────────────

        public override int Length => _codes.Length;

        /// <summary>
        /// Lista de categorías únicas en orden de primera aparición.
        /// Inmutable desde fuera: no se puede añadir ni quitar sin reconstruir la columna.
        /// </summary>
        public ReadOnlyCollection<string> Categories => _categories.AsReadOnly();

        /// <summary>Número de categorías únicas (sin contar null).</summary>
        public int CategoryCount => _categories.Count;

        // ── Constructores ─────────────────────────────────────────────────────

        /// <summary>
        /// Construye desde un array de strings.
        /// Las categorías se detectan automáticamente en orden de primera aparición.
        /// null en el array → celda nula (código -1).
        /// </summary>
        public CategoricalColumn(string name, string[] data) : base(name)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));

            _categories = new List<string>();
            _categoryToCode = new Dictionary<string, int>(StringComparer.Ordinal);
            _codes = new int[data.Length];

            for (int i = 0; i < data.Length; i++)
            {
                var value = data[i];

                if (value == null)
                {
                    _codes[i] = -1;   // nulo
                    continue;
                }

                if (!_categoryToCode.TryGetValue(value, out int code))
                {
                    code = _categories.Count;
                    _categories.Add(value);
                    _categoryToCode[value] = code;
                }

                _codes[i] = code;
            }
        }

        /// <summary>
        /// Constructor interno usado por Filter y otras operaciones.
        /// Recibe los códigos ya calculados y el diccionario ya construido.
        /// No reclasifica: es O(n) en lugar de O(n·m).
        /// </summary>
        private CategoricalColumn(
            string name,
            int[] codes,
            List<string> categories,
            Dictionary<string, int> categoryToCode) : base(name)
        {
            _codes = codes;
            _categories = categories;
            _categoryToCode = categoryToCode;
        }

        // ── Conversión desde StringColumn ─────────────────────────────────────

        /// <summary>
        /// Convierte una StringColumn existente en CategoricalColumn.
        /// Útil tras cargar datos cuando se detecta alta repetición de valores.
        /// Recomendado cuando CardinalityRatio &lt; 0.5 (menos del 50% de valores son únicos).
        /// </summary>
        public static CategoricalColumn FromStringColumn(StringColumn source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            var data = new string[source.Length];
            for (int i = 0; i < source.Length; i++)
                data[i] = source[i];   // null si nulo — el constructor lo maneja

            return new CategoricalColumn(source.Name, data);
        }

        // ── Indexer ───────────────────────────────────────────────────────────

        /// <summary>
        /// Devuelve el string de la categoría, o null si la celda es nula.
        ///
        /// El setter es internal: desde fuera del ensamblado esta propiedad
        /// es de solo lectura, lo que refleja correctamente la semántica inmutable
        /// de CategoricalColumn para consumidores externos.
        ///
        /// Desde dentro del ensamblado, el setter solo acepta valores que ya sean
        /// categorías conocidas o null. Para añadir una categoría nueva desde fuera,
        /// usa ToStringColumn(), modifica y reconstruye.
        /// </summary>
        public string this[int index]
        {
            get
            {
                ValidateIndex(index);
                int code = _codes[index];
                return code == -1 ? null : _categories[code];
            }
            internal set
            {
                ValidateIndex(index);

                if (value == null)
                {
                    _codes[index] = -1;
                    return;
                }

                if (!_categoryToCode.TryGetValue(value, out int code))
                    throw new InvalidOperationException(
                        $"Value '{value}' is not a known category in column '{Name}'. " +
                        $"Known categories: [{string.Join(", ", _categories)}]. " +
                        $"To add new categories, reconstruct the column from a string[].");

                _codes[index] = code;
            }
        }

        // ── Acceso a códigos crudos ───────────────────────────────────────────

        /// <summary>
        /// Devuelve el código entero de la fila.
        /// -1 = nulo. Úsalo en GroupBy y operaciones que agrupan por código,
        /// evitando la descodificación a string.
        /// </summary>
        public int GetCode(int index)
        {
            ValidateIndex(index);
            return _codes[index];
        }

        /// <summary>
        /// Traduce un código a su string. Útil en GroupBy para obtener la clave final.
        /// Devuelve null si code == -1.
        /// </summary>
        public string DecodeCategory(int code)
        {
            if (code == -1) return null;
            if ((uint)code >= (uint)_categories.Count)
                throw new ArgumentOutOfRangeException(nameof(code),
                    $"Code {code} is out of range (0..{_categories.Count - 1}).");
            return _categories[code];
        }

        /// <summary>
        /// Busca el código de un string conocido.
        /// Devuelve true + código si existe; false si no es categoría conocida.
        /// null siempre devuelve true con código -1 (nulo).
        /// Útil para filtros vectorizados: comparar código entero en lugar de string.
        /// </summary>
        public bool TryGetCode(string value, out int code)
        {
            if (value == null) { code = -1; return true; }
            return _categoryToCode.TryGetValue(value, out code);
        }

        /// <summary>
        /// Devuelve una máscara booleana donde true = celda igual al valor dado.
        /// Eficiente: compara enteros (códigos) en lugar de strings.
        /// Si el valor no es una categoría conocida, devuelve todos false sin iterar.
        /// </summary>
        public bool[] EqualsMask(string value)
        {
            var mask = new bool[_codes.Length];

            // Si el valor no existe como categoría, la máscara queda toda a false
            if (!TryGetCode(value, out int targetCode))
                return mask;

            for (int i = 0; i < _codes.Length; i++)
                mask[i] = (_codes[i] == targetCode);

            return mask;
        }

        /// <summary>
        /// Versión multi-valor: equivalente a pandas isin().
        /// df.Where(col.IsInMask("Madrid", "Barcelona", "Valencia"))
        /// </summary>
        public bool[] IsInMask(params string[] values)
        {
            var mask = new bool[_codes.Length];
            if (values == null || values.Length == 0) return mask;

            // Construir set de códigos objetivo — O(k) donde k = valores buscados
            var targetCodes = new HashSet<int>();
            foreach (var v in values)
            {
                if (TryGetCode(v, out int code))
                    targetCodes.Add(code);
            }

            if (targetCodes.Count == 0) return mask;

            // Comparar enteros — O(n), más rápido que comparar strings
            for (int i = 0; i < _codes.Length; i++)
                mask[i] = targetCodes.Contains(_codes[i]);

            return mask;
        }

        // ── Implementación de BaseColumn ──────────────────────────────────────

        public override bool IsNull(int index)
        {
            ValidateIndex(index);
            return _codes[index] == -1;
        }

        /// <inheritdoc/>
        public override object GetBoxed(int index)
        {
            ValidateIndex(index);
            int code = _codes[index];
            return code == -1 ? null : (object)_categories[code];
        }

        /// <inheritdoc/>
        /// Preserva el diccionario de categorías intacto (sin recalcular).
        /// Las categorías con 0 ocurrencias en el resultado se mantienen,
        /// igual que pandas — para que merge/join entre columnas del mismo dominio funcione.
        public override BaseColumn Filter(bool[] mask)
        {
            ValidateMask(mask);

            var resultCodes = new List<int>(capacity: mask.Length);
            for (int i = 0; i < mask.Length; i++)
                if (mask[i]) resultCodes.Add(_codes[i]);

            // Reutilizamos el mismo diccionario — es inmutable y compartirlo es seguro
            return new CategoricalColumn(
                Name,
                resultCodes.ToArray(),
                _categories,        // mismo objeto: O(1), sin copia
                _categoryToCode);
        }

        /// <summary>
        /// Construye una nueva CategoricalColumn recogiendo las filas indicadas por
        /// <paramref name="indices"/>, reutilizando el mismo diccionario de categorías.
        ///
        /// indices[i] == -1 → celda nula en el resultado (fila sin pareja en un join).
        ///
        /// Equivalente a Filter pero con índices arbitrarios en lugar de máscara bool.
        /// Úsalo desde operaciones internas (Merge, etc.) para evitar el ciclo
        /// decode-a-string → recode-a-int que haría GatherColumn genérico.
        /// Es O(n): mapeo directo de código a código, sin tocar _categories.
        /// </summary>
        internal CategoricalColumn GatherByIndices(string name, int[] indices)
        {
            if (indices == null) throw new ArgumentNullException(nameof(indices));

            var resultCodes = new int[indices.Length];
            for (int i = 0; i < indices.Length; i++)
                resultCodes[i] = indices[i] == -1 ? -1 : _codes[indices[i]];

            // Reutilizamos el mismo diccionario — es inmutable y compartirlo es seguro
            return new CategoricalColumn(
                name,
                resultCodes,
                _categories,        // mismo objeto: O(1), sin copia
                _categoryToCode);
        }

        // ── Conversión de vuelta a StringColumn ───────────────────────────────

        /// <summary>
        /// Descodifica la columna a StringColumn.
        /// Útil antes de exportar a CSV/Excel o antes de operaciones de texto libre.
        /// Es O(n): recorre cada celda y sustituye el código por el string.
        /// </summary>
        public StringColumn ToStringColumn()
        {
            var data = new string[_codes.Length];
            for (int i = 0; i < _codes.Length; i++)
            {
                int code = _codes[i];
                data[i] = code == -1 ? null : _categories[code];
            }
            return new StringColumn(Name, data);
        }

        // ── Estadísticas ──────────────────────────────────────────────────────

        /// <summary>
        /// Cuenta las ocurrencias de cada categoría.
        /// Devuelve un diccionario categoría → conteo, en orden de primera aparición.
        /// Las celdas nulas no se cuentan (como pandas value_counts por defecto).
        /// Las categorías con 0 ocurrencias sí aparecen en el resultado.
        /// </summary>
        public Dictionary<string, int> ValueCounts()
        {
            var counts = new Dictionary<string, int>(_categories.Count, StringComparer.Ordinal);

            foreach (var cat in _categories)
                counts[cat] = 0;   // inicializar todas a 0

            foreach (int code in _codes)
            {
                if (code != -1)
                    counts[_categories[code]]++;
            }

            return counts;
        }

        /// <summary>
        /// Ratio de cardinalidad: valores únicos / total de filas.
        /// Útil para decidir si vale la pena convertir una StringColumn a Categorical.
        /// Regla práctica: si CardinalityRatio &lt; 0.5 → CategoricalColumn es beneficiosa.
        /// </summary>
        public double CardinalityRatio()
        {
            if (_codes.Length == 0) return 0.0;
            return (double)_categories.Count / _codes.Length;
        }

        // ── IEnumerable<string> ───────────────────────────────────────────────

        public IEnumerator<string> GetEnumerator()
        {
            for (int i = 0; i < _codes.Length; i++)
            {
                int code = _codes[i];
                yield return code == -1 ? null : _categories[code];
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        // ── Helpers privados ──────────────────────────────────────────────────

        private void ValidateIndex(int index)
        {
            if ((uint)index >= (uint)_codes.Length)
                throw new ArgumentOutOfRangeException(nameof(index),
                    $"Index {index} is out of range for column '{Name}' (length {_codes.Length}).");
        }
    }
}