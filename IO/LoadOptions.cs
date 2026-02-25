// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace MiniPandas.Core.IO
{
    /// <summary>
    /// Opciones de carga para ExcelLoader (y futuros CsvLoader, JsonLoader, etc.).
    /// Inmutable tras construcción: segura para compartir entre hilos.
    ///
    /// MIGRACIÓN DESDE ESTADO GLOBAL:
    ///   Antes: SchemaInference.CategoricalThreshold = 0.3;
    ///   Ahora: var opts = new LoadOptions { CategoricalThreshold = 0.3 };
    ///          ExcelLoader.LoadExcel(path, opts);
    ///
    /// INSTANCIA POR DEFECTO:
    ///   LoadOptions.Default reproduce el comportamiento anterior al cambio,
    ///   por lo que código que no pase opciones explícitas sigue funcionando igual.
    /// </summary>
    public class LoadOptions
    {
        // ── Instancia por defecto (singleton inmutable) ───────────────────────

        /// <summary>
        /// Opciones con los valores por defecto históricos del proyecto.
        /// Usar cuando no se necesita personalización.
        /// </summary>
        public static readonly LoadOptions Default = new LoadOptions();

        // ── Propiedades ───────────────────────────────────────────────────────

        /// <summary>
        /// Si el ratio (valores únicos / total filas) de una columna string es menor
        /// que este umbral, se construye como CategoricalColumn en lugar de StringColumn.
        ///
        /// Rango válido: [0.0, 1.0].
        ///   0.0 → nunca usar CategoricalColumn.
        ///   1.0 → siempre usar CategoricalColumn para columnas string.
        ///   0.5 → valor por defecto (50% de cardinalidad máxima para categorizar).
        ///
        /// Regla práctica:
        ///   Columnas con pocos valores distintos (país, estado, sexo): 0.1–0.3.
        ///   Texto semi-libre (nombres, referencias): 0.5–0.8.
        ///   IDs únicos, texto libre: 0.0 (deshabilitar).
        /// </summary>
        public double CategoricalThreshold { get; }

        /// <summary>
        /// Si true, la primera fila del fichero se interpreta como cabecera.
        /// Si false, las columnas se nombran Column0, Column1, etc.
        /// Por defecto: true.
        /// </summary>
        public bool HasHeader { get; }

        // ── Constructor ───────────────────────────────────────────────────────

        /// <summary>
        /// Crea una instancia con los valores indicados.
        /// Todos los parámetros tienen valores por defecto que reproducen el
        /// comportamiento histórico del proyecto.
        /// </summary>
        /// <param name="categoricalThreshold">
        /// Umbral de cardinalidad para detección automática de categóricas.
        /// Debe estar en [0.0, 1.0]. Por defecto 0.5.
        /// </param>
        /// <param name="hasHeader">
        /// Indica si el fichero tiene fila de cabecera. Por defecto true.
        /// </param>
        public LoadOptions(double categoricalThreshold = 0.5, bool hasHeader = true)
        {
            if (categoricalThreshold < 0.0 || categoricalThreshold > 1.0)
                throw new ArgumentOutOfRangeException(
                    nameof(categoricalThreshold),
                    $"CategoricalThreshold must be in [0.0, 1.0], got {categoricalThreshold}.");

            CategoricalThreshold = categoricalThreshold;
            HasHeader = hasHeader;
        }
    }
}