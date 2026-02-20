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

        protected BaseColumn(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Column name cannot be empty.", nameof(name));
            Name = name;
        }
    }
}
