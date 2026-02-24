namespace MiniPandas.Core.Operations.GroupBy
{
    /// <summary>
    /// Funciones de agregación disponibles en GroupBy.Agg().
    /// </summary>
    public enum AggFunc
    {
        /// <summary>Suma. Solo columnas numéricas. Ignora nulls.</summary>
        Sum,

        /// <summary>Media aritmética. Solo columnas numéricas. Ignora nulls.</summary>
        Mean,

        /// <summary>Número de celdas no nulas.</summary>
        Count,

        /// <summary>Valor mínimo. Solo columnas numéricas. Ignora nulls.</summary>
        Min,

        /// <summary>Valor máximo. Solo columnas numéricas. Ignora nulls.</summary>
        Max,

        /// <summary>Desviación estándar muestral (n-1). Solo columnas numéricas. Ignora nulls.</summary>
        Std,

        /// <summary>Primer valor no nulo del grupo.</summary>
        First,

        /// <summary>Último valor no nulo del grupo.</summary>
        Last,

        /// <summary>Número de valores únicos (ignora nulls).</summary>
        NUnique,

        /// <summary>Varianza muestral (n-1). Solo numéricas.</summary>
        Var,

        /// <summary>Producto de los elementos. Solo numéricas.</summary>
        Prod,

        /// <summary>Mediana (valor central). Solo numéricas.</summary>
        Median
    }
}