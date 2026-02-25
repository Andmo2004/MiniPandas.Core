// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

namespace MiniPandas.Core.Operations.Merge
{
    /// <summary>
    /// Tipo de join para DataFrame.Merge().
    /// Equivalente directo al parámetro 'how' de pandas df.merge().
    /// </summary>
    public enum JoinType
    {
        /// <summary>
        /// Solo filas con clave presente en ambos DataFrames.
        /// Las filas sin pareja se descartan.
        /// Equivalente a SQL INNER JOIN / pandas how='inner'.
        /// </summary>
        Inner,

        /// <summary>
        /// Todas las filas del DataFrame izquierdo.
        /// Si no hay coincidencia en el derecho, las columnas derechas son null.
        /// Equivalente a SQL LEFT JOIN / pandas how='left'.
        /// </summary>
        Left,

        /// <summary>
        /// Todas las filas del DataFrame derecho.
        /// Si no hay coincidencia en el izquierdo, las columnas izquierdas son null.
        /// Equivalente a SQL RIGHT JOIN / pandas how='right'.
        /// </summary>
        Right,

        /// <summary>
        /// Todas las filas de ambos DataFrames.
        /// Las celdas sin coincidencia son null en ambos lados.
        /// Equivalente a SQL FULL OUTER JOIN / pandas how='outer'.
        /// </summary>
        Outer
    }
}