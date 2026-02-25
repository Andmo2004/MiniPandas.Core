// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.Operations
{
    /// <summary>
    /// Reconstruye columnas recogiendo filas por índices arbitrarios.
    ///
    /// ORIGEN:
    ///   Esta lógica vivía duplicada en MergeOp.GatherColumn (privado estático).
    ///   Se extrae aquí para que GroupByContext.Apply() pueda materializar grupos
    ///   directamente desde índices, sin construir una máscara booleana del tamaño
    ///   total del DataFrame por cada grupo.
    ///
    /// SEMÁNTICA DE índices:
    ///   - Índice válido (>= 0): copia el valor de esa fila.
    ///   - Índice -1: celda nula en el resultado (usado por MergeOp para outer joins).
    ///
    /// TIPOS SOPORTADOS:
    ///   DataColumn&lt;double&gt;, DataColumn&lt;int&gt;, DataColumn&lt;DateTime&gt;, DataColumn&lt;bool&gt;,
    ///   StringColumn, CategoricalColumn.
    ///   Para añadir un nuevo tipo: añade un bloque 'if' en GatherColumn.
    /// </summary>
    internal static class ColumnGather
    {
        /// <summary>
        /// Construye una nueva columna del mismo tipo que <paramref name="source"/>
        /// recogiendo los valores en las posiciones indicadas por <paramref name="indices"/>.
        /// </summary>
        /// <param name="source">Columna origen.</param>
        /// <param name="indices">
        /// Índices de las filas a recoger. -1 produce celda nula.
        /// No tiene que ser un subconjunto ordenado ni contiguo.
        /// </param>
        /// <param name="name">
        /// Nombre de la columna resultado. Si es null, hereda el nombre de <paramref name="source"/>.
        /// </param>
        internal static BaseColumn Gather(BaseColumn source, int[] indices, string name = null)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (indices == null) throw new ArgumentNullException(nameof(indices));

            string colName = name ?? source.Name;
            int n = indices.Length;

            if (source is DataColumn<double> dcD)
            {
                var data = new double[n];
                var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1 || dcD.IsNull(indices[i])) { nulls[i] = true; continue; }
                    data[i] = dcD.GetRawValue(indices[i]);
                }
                return new DataColumn<double>(colName, data, nulls);
            }

            if (source is DataColumn<int> dcI)
            {
                var data = new int[n];
                var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1 || dcI.IsNull(indices[i])) { nulls[i] = true; continue; }
                    data[i] = dcI.GetRawValue(indices[i]);
                }
                return new DataColumn<int>(colName, data, nulls);
            }

            if (source is DataColumn<DateTime> dcDt)
            {
                var data = new DateTime[n];
                var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1 || dcDt.IsNull(indices[i])) { nulls[i] = true; continue; }
                    data[i] = dcDt.GetRawValue(indices[i]);
                }
                return new DataColumn<DateTime>(colName, data, nulls);
            }

            if (source is DataColumn<bool> dcB)
            {
                var data = new bool[n];
                var nulls = new bool[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1 || dcB.IsNull(indices[i])) { nulls[i] = true; continue; }
                    data[i] = dcB.GetRawValue(indices[i]);
                }
                return new DataColumn<bool>(colName, data, nulls);
            }

            if (source is StringColumn sc)
            {
                var data = new string[n];
                for (int i = 0; i < n; i++)
                {
                    if (indices[i] == -1) continue;   // null permanece null
                    data[i] = sc[indices[i]];
                }
                return new StringColumn(colName, data);
            }

            if (source is CategoricalColumn cc)
            {
                // GatherByIndices reutiliza el diccionario de categorías por referencia
                // sin decode a string ni recode de vuelta — O(n) puro.
                return cc.GatherByIndices(colName, indices);
            }

            throw new InvalidOperationException(
                $"ColumnGather.Gather: unsupported column type '{source.GetType().Name}' " +
                $"for column '{source.Name}'. " +
                $"Add an 'if' block in ColumnGather.Gather.");
        }
    }
}