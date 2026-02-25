// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using ExcelDataReader;

namespace MiniPandas.Core.IO.Excel
{
    /// <summary>
    /// Loader de ficheros Excel (.xlsx, .xls). Implementa IDataLoader.
    ///
    /// MIGRACIÓN DESDE IO/ExcelLoader.cs:
    ///   El namespace cambia de MiniPandas.Core.IO a MiniPandas.Core.IO.Excel.
    ///   La API es idéntica; la lógica interna no cambia.
    /// </summary>
    public sealed class ExcelLoader : IDataLoader
    {
        public DataFrame Load(string path, LoadOptions options = null)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be null or empty.", nameof(path));
            if (!File.Exists(path))
                throw new FileNotFoundException("Excel file not found.", path);

            var opts = options ?? LoadOptions.Default;

            using (var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var reader = ExcelReaderFactory.CreateReader(stream))
            {
                // En ExcelDataReader el reader arranca ANTES de la primera hoja.
                // NextResult() lo posiciona en sheet1; sin esta llamada FieldCount == 0
                // y el primer Read() devuelve false → "The Excel file is empty."
                if (!reader.NextResult())
                    throw new InvalidDataException("The Excel file is empty.");

                // ── 1. Leer nombres de columna ────────────────────────────────
                if (!reader.Read())
                    throw new InvalidDataException("The Excel file is empty.");

                int colCount = reader.FieldCount;
                var names = new string[colCount];

                for (int i = 0; i < colCount; i++)
                {
                    names[i] = opts.HasHeader
                        ? (reader.GetValue(i)?.ToString()?.Trim() ?? $"Column{i}")
                        : $"Column{i}";
                }

                if (!opts.HasHeader)
                {
                    reader.Reset();
                    reader.Read();
                }

                // ── 2. Leer filas ─────────────────────────────────────────────
                var rawRows = new List<object[]>(capacity: 256);

                while (reader.Read())
                {
                    var row = new object[colCount];
                    bool isEmpty = true;

                    for (int i = 0; i < colCount; i++)
                    {
                        var val = reader.GetValue(i);
                        row[i] = (val == DBNull.Value) ? null : val;
                        if (isEmpty && row[i] != null) isEmpty = false;
                    }

                    if (!isEmpty)
                        rawRows.Add(row);
                }

                // ── 3. Inferencia de tipos ────────────────────────────────────
                var df = new DataFrame(rawRows.Count);
                foreach (var column in MiniPandas.Core.IO.Core.SchemaInference.InferColumns(names, rawRows, opts))
                    df.AddColumn(column);

                return df;
            }
        }
    }
}