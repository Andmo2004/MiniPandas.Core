// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Globalization;
using System.Collections.Generic;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.IO.Excel
{
    /// <summary>
    /// Exportador de DataFrames a ficheros Excel (.xlsx).
    ///
    /// IMPLEMENTACIÓN SIN DEPENDENCIAS EXTERNAS:
    ///   Genera un fichero .xlsx válido usando el formato Open XML (OOXML) directamente,
    ///   que es un ZIP con ficheros XML internos. No requiere Office ni librerías de pago.
    ///   Compatible con Excel 2007+, LibreOffice y Google Sheets.
    ///
    /// TIPOS SOPORTADOS:
    ///   double / int     → número en Excel (formato numérico estándar)
    ///   DateTime         → fecha Excel (número serial + formato de fecha)
    ///   bool             → TRUE / FALSE como string (Excel no tiene tipo booleano nativo en OOXML básico)
    ///   string / Categorical → texto
    ///   null             → celda vacía
    ///
    /// USO:
    ///   ExcelExporter.Write(df, "salida.xlsx");
    ///   ExcelExporter.Write(df, "salida.xlsx", new ExcelOptions(sheetName: "Ventas"));
    /// </summary>
    public static class ExcelExporter
    {
        /// <summary>
        /// Escribe un DataFrame en un fichero .xlsx.
        /// </summary>
        public static void Write(DataFrame df, string path, ExcelOptions options = null)
        {
            if (df == null) throw new ArgumentNullException(nameof(df));
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be null or empty.", nameof(path));

            var opts = options ?? ExcelOptions.Default;

            // Asegurar extensión .xlsx
            if (!path.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
                path += ".xlsx";

            var columns = new List<BaseColumn>(df.Columns);

            // ── Construir el contenido XML de la hoja ─────────────────────────
            string sheetXml = BuildSheetXml(df, columns, opts);
            //string sharedStringsXml = BuildSharedStringsXml(new List<string>());

            // ── Empaquetar como ZIP (.xlsx = Open XML) ────────────────────────
            //WriteXlsx(path, opts.SheetName, sheetXml, sharedStringsXml);
            WriteXlsx(path, opts.SheetName, sheetXml);
        }

        // ── Construcción del XML de la hoja ──────────────────────────────────

        private static string BuildSheetXml(
            DataFrame df, List<BaseColumn> columns, ExcelOptions opts)
        {
            int lastCol = columns.Count - 1;
            int lastRow = df.RowCount + 1;
            string lastCell = CellRef(lastCol, lastRow);

            var sb = new System.Text.StringBuilder();
            sb.Append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
            sb.Append("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">");
            sb.Append($"<dimension ref=\"A1:{lastCell}\"/>");
            sb.Append("<sheetData>");

            // ── Fila de cabecera ──────────────────────────────────────────────
            sb.Append("<row r=\"1\">");
            for (int c = 0; c < columns.Count; c++)
            {
                string cellRef = CellRef(c, 1);
                string escaped = XmlEscape(columns[c].Name);
                sb.Append($"<c r=\"{cellRef}\" t=\"inlineStr\"><is><t>{escaped}</t></is></c>");
            }
            sb.Append("</row>");

            // ── Filas de datos ────────────────────────────────────────────────
            for (int row = 0; row < df.RowCount; row++)
            {
                int excelRow = row + 2;   // +1 cabecera, +1 base-1
                sb.Append($"<row r=\"{excelRow}\">");

                for (int c = 0; c < columns.Count; c++)
                {
                    string cellRef = CellRef(c, excelRow);
                    AppendCell(sb, columns[c], row, cellRef);
                }

                sb.Append("</row>");
            }

            sb.Append("</sheetData>");
            sb.Append("</worksheet>");
            return sb.ToString();
        }

        private static void AppendCell(
            System.Text.StringBuilder sb, BaseColumn column, int row, string cellRef)
        {
            if (column.IsNull(row))
                return;   // celda vacía: no escribir nada

            if (column is DataColumn<double> dcD)
            {
                double v = dcD.GetRawValue(row);
                if (double.IsNaN(v) || double.IsInfinity(v)) return;
                sb.Append($"<c r=\"{cellRef}\"><v>{v.ToString("G17", CultureInfo.InvariantCulture)}</v></c>");
            }
            else if (column is DataColumn<int> dcI)
            {
                sb.Append($"<c r=\"{cellRef}\"><v>{dcI.GetRawValue(row)}</v></c>");
            }
            else if (column is DataColumn<DateTime> dcDt)
            {
                // Excel almacena fechas como número serial (días desde 1900-01-00)
                double serial = ToExcelSerial(dcDt.GetRawValue(row));
                // Estilo 14 = formato de fecha "mm-dd-yy" nativo de Excel.
                // Sin estilos completos usamos ISO 8601 como string para máxima compatibilidad.
                string iso = dcDt.GetRawValue(row).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                sb.Append($"<c r=\"{cellRef}\" t=\"inlineStr\"><is><t>{iso}</t></is></c>");
            }
            else if (column is DataColumn<bool> dcB)
            {
                string val = dcB.GetRawValue(row) ? "TRUE" : "FALSE";
                sb.Append($"<c r=\"{cellRef}\" t=\"inlineStr\"><is><t>{val}</t></is></c>");
            }
            else
            {
                // StringColumn, CategoricalColumn, fallback
                string text = column.GetBoxed(row)?.ToString() ?? string.Empty;
                sb.Append($"<c r=\"{cellRef}\" t=\"inlineStr\"><is><t>{XmlEscape(text)}</t></is></c>");
            }
        }

        // ── Empaquetado ZIP / Open XML ────────────────────────────────────────

        private static void WriteXlsx(
            string path, string sheetName, string sheetXml)
        //string path, string sheetName, string sheetXml, string sharedStringsXml)
        {
            // Un .xlsx es un ZIP con una estructura fija de ficheros XML
            using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write))
            using (var zip = new System.IO.Compression.ZipArchive(fs, System.IO.Compression.ZipArchiveMode.Create))
            {
                WriteEntry(zip, "[Content_Types].xml", BuildContentTypes(sheetName));
                WriteEntry(zip, "_rels/.rels", BuildRels());
                WriteEntry(zip, "xl/workbook.xml", BuildWorkbook(sheetName));
                WriteEntry(zip, "xl/_rels/workbook.xml.rels", BuildWorkbookRels());
                WriteEntry(zip, "xl/worksheets/sheet1.xml", sheetXml);
                //WriteEntry(zip, "xl/sharedStrings.xml", sharedStringsXml);
            }
        }

        private static void WriteEntry(
            System.IO.Compression.ZipArchive zip, string entryName, string content)
        {
            var entry = zip.CreateEntry(entryName, System.IO.Compression.CompressionLevel.Optimal);
            using (var sw = new StreamWriter(entry.Open(), new System.Text.UTF8Encoding(false)))
                sw.Write(content);
        }

        // ── XML de soporte Open XML ───────────────────────────────────────────

        private static string BuildContentTypes(string sheetName) =>
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">" +
            "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>" +
            "<Default Extension=\"xml\" ContentType=\"application/xml\"/>" +
            "<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>" +
            "<Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>" +
            //"<Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/>" +
            "</Types>";

        private static string BuildRels() =>
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">" +
            "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>" +
            "</Relationships>";

        private static string BuildWorkbook(string sheetName) =>
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" " +
            "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">" +
            "<sheets>" +
            $"<sheet name=\"{XmlEscape(sheetName)}\" sheetId=\"1\" r:id=\"rId1\"/>" +
            "</sheets>" +
            "</workbook>";

        private static string BuildWorkbookRels() =>
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">" +
            "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"/xl/worksheets/sheet1.xml\"/>" +
            //"<Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings\" Target=\"sharedStrings.xml\"/>" +
            "</Relationships>";

        //private static string BuildSharedStringsXml(List<string> strings) =>
        //    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
        //    "<sst xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/sheet/2006/main\" count=\"0\" uniqueCount=\"0\"/>";

        // ── Helpers ───────────────────────────────────────────────────────────

        /// <summary>
        /// Convierte índice de columna (base-0) y fila (base-1) a referencia Excel (A1, B3...).
        /// Soporta hasta columna ZZ (702 columnas).
        /// </summary>
        private static string CellRef(int colIndex, int rowIndex)
        {
            string col = ColLetter(colIndex);
            return $"{col}{rowIndex}";
        }

        private static string ColLetter(int index)
        {
            if (index < 26)
                return ((char)('A' + index)).ToString();

            int high = index / 26 - 1;
            int low = index % 26;
            return ((char)('A' + high)).ToString() + (char)('A' + low);
        }

        /// <summary>
        /// Número serial de Excel: días desde 1900-01-01 (con el bug histórico del año 1900).
        /// </summary>
        private static double ToExcelSerial(DateTime dt)
        {
            var epoch = new DateTime(1899, 12, 30);
            return (dt - epoch).TotalDays;
        }

        private static string XmlEscape(string value)
        {
            if (value == null) return string.Empty;
            return value
                .Replace("&", "&amp;")
                .Replace("<", "&lt;")
                .Replace(">", "&gt;")
                .Replace("\"", "&quot;")
                .Replace("'", "&apos;");
        }
    }

    /// <summary>
    /// Opciones para ExcelExporter.
    /// </summary>
    public sealed class ExcelOptions
    {
        public static readonly ExcelOptions Default = new ExcelOptions();

        /// <summary>Nombre de la hoja en el fichero Excel. Por defecto "Sheet1".</summary>
        public string SheetName { get; }

        public ExcelOptions(string sheetName = "Sheet1")
        {
            if (string.IsNullOrWhiteSpace(sheetName))
                throw new ArgumentException("SheetName cannot be empty.", nameof(sheetName));
            SheetName = sheetName;
        }
    }
}