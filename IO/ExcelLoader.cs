using System;
using System.Collections.Generic;
using System.IO;
using ExcelDataReader;
using MiniPandas.Core.Columns;

namespace MiniPandas.Core.IO
{
    public static class ExcelLoader
    {
        public static DataFrame LoadExcel(string path, bool hasHeader = true)
        {
            if (!File.Exists(path))
                throw new FileNotFoundException("Excel file not found.", path);

            using (var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var reader = ExcelReaderFactory.CreateReader(stream))
            {
                // ── 1. Leer nombres de columna ───────────────────────────────
                if (!reader.Read())
                    throw new InvalidDataException("The Excel file is empty.");

                int colCount = reader.FieldCount;
                var names = new string[colCount];

                for (int i = 0; i < colCount; i++)
                {
                    names[i] = hasHeader
                        ? (reader.GetValue(i)?.ToString()?.Trim() ?? $"Column{i}")
                        : $"Column{i}";
                }

                // Si no hay encabezado, la primera fila son datos: volvemos al inicio
                if (!hasHeader)
                {
                    reader.Reset();
                    reader.Read();
                }

                // ── 2. Leer todas las filas en memoria ───────────────────────
                // Usamos object[][] para no pre-comprometernos con tipos ni tamaño.
                // reader.RowCount es poco fiable en .xls y en sheets con celdas fantasma.
                var rawRows = new List<object[]>(capacity: 256);

                while (reader.Read())
                {
                    var row = new object[colCount];
                    bool isEmpty = true;

                    for (int i = 0; i < colCount; i++)
                    {
                        var val = reader.GetValue(i);
                        // Normalizamos DBNull a null desde el origen
                        row[i] = (val == DBNull.Value) ? null : val;
                        if (isEmpty && row[i] != null) isEmpty = false;
                    }

                    if (!isEmpty)   // descartamos filas completamente vacías
                        rawRows.Add(row);
                }

                // ── 3. Delegar inferencia y construcción en SchemaInference ──
                // Fix: la lógica de inferencia vivía duplicada aquí y en SchemaInference.
                // Ahora ExcelLoader solo lee bytes; SchemaInference decide los tipos.
                var df = new DataFrame(rawRows.Count);

                foreach (var column in SchemaInference.InferColumns(names, rawRows))
                    df.AddColumn(column);

                return df;
            }
        }
    }
}