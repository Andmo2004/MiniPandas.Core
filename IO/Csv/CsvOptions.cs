using System;
using System.Text;

namespace MiniPandas.Core.IO.Csv
{
    /// <summary>
    /// Opciones de carga y exportación para ficheros CSV.
    /// Extiende LoadOptions añadiendo propiedades específicas del formato CSV.
    ///
    /// SEPARADORES HABITUALES:
    ///   ','  — estándar internacional (RFC 4180), Excel en configuración en-US.
    ///   ';'  — estándar en Europa continental (es-ES, de-DE, fr-FR):
    ///          Excel usa ';' cuando el separador decimal del sistema es ','.
    ///   '\t' — TSV (Tab-Separated Values), común en exports de bases de datos.
    ///
    /// PRESETS:
    ///   CsvOptions.Default     → ',' con cabecera, UTF-8, sin BOM.
    ///   CsvOptions.European    → ';' con cabecera, UTF-8, sin BOM.
    ///   CsvOptions.Tab         → '\t' con cabecera, UTF-8, sin BOM.
    /// </summary>
    public sealed class CsvOptions : LoadOptions
    {
        // ── Presets ───────────────────────────────────────────────────────────

        /// <summary>Coma como separador. Comportamiento por defecto internacional.</summary>
        public new static readonly CsvOptions Default = new CsvOptions();

        /// <summary>Punto y coma como separador. Habitual en Europa continental.</summary>
        public static readonly CsvOptions European = new CsvOptions(delimiter: ';');

        /// <summary>Tabulador como separador (TSV).</summary>
        public static readonly CsvOptions Tab = new CsvOptions(delimiter: '\t');

        // ── Propiedades ───────────────────────────────────────────────────────

        /// <summary>
        /// Carácter separador de campos.
        /// Por defecto ',' (RFC 4180).
        /// </summary>
        public char Delimiter { get; }

        /// <summary>
        /// Carácter usado para delimitar campos que contienen el separador o saltos de línea.
        /// Por defecto '"' (RFC 4180).
        /// </summary>
        public char QuoteChar { get; }

        /// <summary>
        /// Encoding del fichero.
        /// Por defecto UTF-8 sin BOM, que es el estándar moderno y compatible con pandas.
        /// Para ficheros legacy de Windows usar Encoding.GetEncoding(1252).
        /// </summary>
        public Encoding Encoding { get; }

        /// <summary>
        /// Si true, las celdas vacías ("") se tratan como null.
        /// Si false, se tratan como string vacío.
        /// Por defecto true (consistente con pandas read_csv).
        /// </summary>
        public bool TreatEmptyAsNull { get; }

        // ── Constructor ───────────────────────────────────────────────────────

        /// <summary>
        /// Crea opciones CSV con los valores indicados.
        /// </summary>
        /// <param name="delimiter">Separador de campos. Por defecto ','.</param>
        /// <param name="quoteChar">Carácter de cita. Por defecto '"'.</param>
        /// <param name="hasHeader">Si hay fila de cabecera. Por defecto true.</param>
        /// <param name="categoricalThreshold">Umbral para columnas categóricas. Por defecto 0.5.</param>
        /// <param name="encoding">Encoding del fichero. Por defecto UTF-8.</param>
        /// <param name="treatEmptyAsNull">Tratar vacío como null. Por defecto true.</param>
        public CsvOptions(
            char delimiter = ',',
            char quoteChar = '"',
            bool hasHeader = true,
            double categoricalThreshold = 0.5,
            Encoding encoding = null,
            bool treatEmptyAsNull = true)
            : base(categoricalThreshold, hasHeader)
        {
            if (delimiter == quoteChar)
                throw new ArgumentException(
                    $"Delimiter ('{delimiter}') and QuoteChar cannot be the same character.");

            Delimiter = delimiter;
            QuoteChar = quoteChar;
            Encoding = encoding ?? new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
            TreatEmptyAsNull = treatEmptyAsNull;
        }
    }
}