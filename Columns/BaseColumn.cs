using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MiniPandas.Core.Columns
{
    public abstract class BaseColumn
    {
        public string Name { get; }  // Inmutable tras construcción

        public abstract int Length { get; }
        public abstract bool IsNull(int index);

        /// <summary>
        /// Devuelve el valor de la celda como object (con boxing).
        /// Uso deliberadamente limitado: display, JSON, comparaciones heterogéneas.
        /// El 99% del código interno usa DataColumn<T> directamente para evitar boxing.
        /// </summary>
        public abstract object GetBoxed(int index);

        /// <summary>
        /// Devuelve una nueva columna con solo las filas donde mask[i] == true.
        /// Siempre produce una nueva instancia (semántica inmutable).
        /// </summary>
        /// <param name="mask">Array de booleanos del mismo largo que la columna.</param>
        public abstract BaseColumn Filter(bool[] mask);

        protected BaseColumn(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Column name cannot be empty.", nameof(name));
            Name = name;
        }

        /// <summary>
        /// Valida que mask no sea null y tenga la misma longitud que la columna.
        /// Llamar desde cada implementación de Filter antes de iterar.
        /// </summary>
        protected void ValidateMask(bool[] mask)
        {
            if (mask == null) throw new ArgumentNullException(nameof(mask));
            if (mask.Length != Length)
                throw new ArgumentException(
                    $"Mask length ({mask.Length}) must match column length ({Length}).",
                    nameof(mask));
        }
    }
}