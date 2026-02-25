using System;

namespace MiniPandas.Core.IO.Json
{
    /// <summary>
    /// Opciones para carga y exportación de ficheros JSON.
    /// Extiende LoadOptions añadiendo la orientación del JSON.
    ///
    /// La orientación define la estructura del JSON:
    ///   Records → [{col:val}, ...]           legible, repetitivo
    ///   Columns → {col:[val,...]}             compacto, ideal para gráficos
    ///   Split   → {columns:[...], data:[[]]} más compacto, ideal para SQL bulk
    ///
    /// Al leer (JsonLoader), la orientación indica cómo está estructurado el fichero.
    /// Al escribir (JsonExporter), indica cómo se quiere estructurar la salida.
    ///
    /// AUTODETECCIÓN:
    ///   JsonLoader puede detectar la orientación automáticamente inspeccionando
    ///   el token raíz del JSON:
    ///     '[' → Records
    ///     '{' con clave "columns" y "data" → Split
    ///     '{' en otro caso → Columns
    ///   Usar JsonOptions.AutoDetect para este comportamiento.
    /// </summary>
    public sealed class JsonOptions : LoadOptions
    {
        // ── Centinela para autodetección ──────────────────────────────────────

        /// <summary>
        /// Valor especial que indica que JsonLoader debe detectar la orientación
        /// automáticamente inspeccionando el token raíz del JSON.
        /// Solo válido al leer; al escribir se debe especificar una orientación concreta.
        /// </summary>
        public const JsonOrientation AutoDetectOrientation = (JsonOrientation)(-1);

        // ── Presets ───────────────────────────────────────────────────────────

        /// <summary>Autodetección de orientación. Preset recomendado para lectura.</summary>
        public static readonly JsonOptions AutoDetect =
            new JsonOptions(AutoDetectOrientation);

        /// <summary>Orientación Records. Compatible con pandas read_json default.</summary>
        public static readonly JsonOptions Records =
            new JsonOptions(JsonOrientation.Records);

        /// <summary>Orientación Columns. Ideal para datos de gráficos.</summary>
        public static readonly JsonOptions Columns =
            new JsonOptions(JsonOrientation.Columns);

        /// <summary>Orientación Split. Más compacta, ideal para SQL bulk.</summary>
        public static readonly JsonOptions Split =
            new JsonOptions(JsonOrientation.Split);

        // ── Propiedades ───────────────────────────────────────────────────────

        /// <summary>
        /// Orientación del JSON.
        /// Usar AutoDetectOrientation (-1) para que JsonLoader la detecte automáticamente.
        /// </summary>
        public JsonOrientation Orientation { get; }

        /// <summary>
        /// Si true, la salida del exporter tendrá indentación legible.
        /// Si false, salida compacta (menor tamaño).
        /// Por defecto true.
        /// </summary>
        public bool Indented { get; }

        // ── Constructor ───────────────────────────────────────────────────────

        /// <summary>
        /// Crea opciones JSON con los valores indicados.
        /// </summary>
        /// <param name="orientation">
        /// Orientación del JSON. Por defecto AutoDetect al leer, Records al escribir.
        /// </param>
        /// <param name="indented">Formato indentado. Por defecto true.</param>
        /// <param name="categoricalThreshold">Umbral para columnas categóricas. Por defecto 0.5.</param>
        public JsonOptions(
            JsonOrientation orientation = AutoDetectOrientation,
            bool indented = true,
            double categoricalThreshold = 0.5)
            : base(categoricalThreshold, hasHeader: true)
        {
            Orientation = orientation;
            Indented = indented;
        }
    }
}