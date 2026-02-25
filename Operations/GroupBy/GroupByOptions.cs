// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace MiniPandas.Core.Operations.GroupBy
{
    /// <summary>
    /// Opciones para GroupBy. Inmutable tras construcción: segura para compartir entre hilos.
    ///
    /// MIGRACIÓN DESDE ESTADO GLOBAL:
    ///   Antes: GroupByContext.KeySeparator = "##";
    ///   Ahora: var opts = new GroupByOptions { KeySeparator = "##" };
    ///          df.GroupBy(opts, "pais", "ciudad");
    ///
    /// INSTANCIA POR DEFECTO:
    ///   GroupByOptions.Default reproduce el comportamiento anterior al cambio.
    /// </summary>
    public sealed class GroupByOptions
    {
        // ── Instancia por defecto (singleton inmutable) ───────────────────────

        /// <summary>
        /// Opciones con los valores por defecto históricos del proyecto.
        /// </summary>
        public static readonly GroupByOptions Default = new GroupByOptions();

        // ── Propiedades ───────────────────────────────────────────────────────

        /// <summary>
        /// Separador usado para construir claves compuestas en GroupBy multi-columna.
        ///
        /// PROBLEMA QUE RESUELVE:
        ///   GroupBy("pais", "ciudad") concatena los valores para formar una clave única:
        ///   "España" + "|" + "Madrid" → "España|Madrid".
        ///   Si tus datos contienen el separador (ej: "España|Norte"), puede haber
        ///   colisiones silenciosas: "España|Norte" + "|" + "Madrid" == "España" + "|" + "Norte|Madrid".
        ///
        /// RECOMENDACIÓN:
        ///   Usa una secuencia que no pueda aparecer en tus datos.
        ///   Para datos generales: "\x00|\x00" (nulo-pipe-nulo) es prácticamente seguro.
        ///   Para datos controlados: el "|" por defecto suele ser suficiente.
        ///   Para máxima seguridad: usa GroupBy de columna única o escapa los valores.
        ///
        /// Por defecto: "|" (comportamiento histórico del proyecto).
        /// </summary>
        public string KeySeparator { get; }

        // ── Constructor ───────────────────────────────────────────────────────

        /// <summary>
        /// Crea una instancia con los valores indicados.
        /// </summary>
        /// <param name="keySeparator">
        /// Separador para claves compuestas. No puede ser null ni vacío.
        /// Por defecto "|".
        /// </param>
        public GroupByOptions(string keySeparator = "|")
        {
            if (string.IsNullOrEmpty(keySeparator))
                throw new ArgumentException(
                    "KeySeparator cannot be null or empty.", nameof(keySeparator));

            KeySeparator = keySeparator;
        }
    }
}