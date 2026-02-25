// Copyright (c) 2025 Andrés Moros Rincón
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace MiniPandas.Core.IO
{
    /// <summary>
    /// Contrato común para todos los loaders de formato.
    ///
    /// DISEÑO:
    ///   Los loaders son instancias (no estáticos) para poder implementar esta interfaz.
    ///   Esto permite:
    ///     - Inyección de dependencias: pasar un IDataLoader sin conocer el formato.
    ///     - Tests: sustituir por un loader falso que devuelve datos fijos.
    ///     - Extensibilidad: añadir ParquetLoader, SqlLoader, etc. sin tocar el código existente.
    ///
    ///   Los exporters, en cambio, son estáticos: su uso es siempre directo y puntual,
    ///   y no se necesita abstraer sobre "qué exporter usar en tiempo de ejecución".
    ///
    /// USO:
    ///   IDataLoader loader = new CsvLoader();
    ///   DataFrame df = loader.Load("datos.csv");
    ///
    ///   // Con opciones específicas del formato:
    ///   IDataLoader loader = new CsvLoader();
    ///   DataFrame df = loader.Load("datos.csv", new CsvOptions(delimiter: ';'));
    ///
    ///   // Genérico (el llamador no conoce el formato):
    ///   DataFrame df = loader.Load(path, LoadOptions.Default);
    /// </summary>
    public interface IDataLoader
    {
        /// <summary>
        /// Carga un fichero y devuelve un DataFrame.
        /// </summary>
        /// <param name="path">Ruta al fichero. No puede ser null o vacío.</param>
        /// <param name="options">
        /// Opciones de carga. Si es null se usan los valores por defecto del loader.
        /// Pasar opciones específicas del formato (CsvOptions, JsonOptions) permite
        /// personalizar el comportamiento sin romper la interfaz genérica.
        /// </param>
        DataFrame Load(string path, LoadOptions options = null);
    }
}