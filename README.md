# MiniPandas.Core

**MiniPandas.Core** es una librería .NET (net472) inspirada en pandas de Python para manipulación tabular de datos. Proporciona un `DataFrame` tipado con columnas fuertemente tipadas, operaciones de filtrado, agrupación, merge y aritmética vectorizada, junto con soporte de carga/exportación para CSV, Excel y JSON.

---

## Tabla de contenidos

- [Arquitectura general](#arquitectura-general)
- [Columnas](#columnas)
  - [BaseColumn](#basecolumn)
  - [DataColumn\<T\>](#datacolumnt)
  - [StringColumn](#stringcolumn)
  - [CategoricalColumn](#categoricalcolumn)
- [DataFrame](#dataframe)
  - [Creación](#creación)
  - [Acceso a columnas](#acceso-a-columnas)
  - [Filtrado](#filtrado)
  - [Head y Tail](#head-y-tail)
  - [Print — Display tabular](#print--display-tabular)
- [Operaciones sobre máscaras — MaskOps](#operaciones-sobre-máscaras--maskops)
- [Aritmética vectorizada — VectorOps](#aritmética-vectorizada--vectorops)
- [GroupBy](#groupby)
  - [Agg](#agg)
  - [Count](#count)
  - [Filter](#filter)
  - [Transform](#transform)
  - [Apply](#apply)
- [Merge (Join)](#merge-join)
- [IO — Carga y exportación](#io--carga-y-exportación)
  - [CSV](#csv)
  - [Excel](#excel)
  - [JSON](#json)
  - [SchemaInference](#schemainference)
- [Ejemplo completo](#ejemplo-completo)
- [Dependencias](#dependencias)

---

## Arquitectura general

```
MiniPandas.Core
├── Columns/
│   ├── BaseColumn.cs           ← contrato abstracto de columna
│   ├── DataColumn.cs           ← columna tipada para tipos valor (double, int, DateTime…)
│   ├── StringColumn.cs         ← columna de strings con null nativo
│   └── CategoricalColumn.cs    ← columna categórica con codificación por enteros
├── DataFrame.cs                ← contenedor principal de columnas
├── DataFramePrint.cs           ← display tabular estilo pandas (partial class)
├── IO/
│   ├── IDataLoader.cs          ← interfaz genérica de loaders
│   ├── LoadOptions.cs          ← opciones base compartidas
│   ├── Core/SchemaInference.cs ← inferencia automática de tipos
│   ├── Csv/                    ← CsvLoader, CsvExporter, CsvOptions
│   ├── Excel/                  ← ExcelLoader, ExcelExporter
│   └── Json/                   ← JsonLoader, JsonExporter, JsonOptions
└── Operations/
    ├── Math/
    │   ├── MaskOps.cs          ← operaciones lógicas sobre bool[]
    │   └── VectorOps.cs        ← aritmética vectorizada sobre columnas
    ├── GroupBy/
    │   ├── GroupByContext.cs   ← resultado de df.GroupBy(...)
    │   ├── Aggregations.cs     ← implementaciones de AggFunc
    │   ├── AggFunc.cs          ← enum de funciones de agregación
    │   ├── ColumnGather.cs     ← recolección de filas por índice sin boxing
    │   └── GroupByOptions.cs   ← configuración del separador de clave
    └── Merge/
        ├── MergeOp.cs          ← hash join entre dos DataFrames
        └── JoinType.cs         ← enum Inner / Left / Right / Outer
```

El flujo típico es: **cargar datos → construir DataFrame → filtrar/transformar → exportar**.

---

## Columnas

Las columnas son el bloque fundamental de la librería. Todas heredan de `BaseColumn` y tienen semántica **inmutable** en sus operaciones de filtrado: cada operación devuelve una nueva instancia sin modificar la original.

### BaseColumn

`BaseColumn` es la clase abstracta que todos los tipos de columna implementan. Define el contrato mínimo para ser almacenada en un `DataFrame`.

```csharp
public abstract class BaseColumn
{
    public string Name { get; }         // Inmutable tras construcción
    public abstract int Length { get; }
    public abstract bool IsNull(int index);
    public abstract object GetBoxed(int index);  // Boxing controlado (display/JSON)
    public abstract BaseColumn Filter(bool[] mask);
}
```

> **Nota de diseño:** `GetBoxed` existe para display, serialización JSON y comparaciones heterogéneas. El 99% del código interno accede a `DataColumn<T>` directamente para evitar el overhead del boxing.

---

### DataColumn\<T\>

Columna tipada para **tipos valor**: `double`, `int`, `DateTime`, `bool`, etc. Internamente almacena dos arrays paralelos: los datos (`T[]`) y una máscara de nulos (`BitArray`).

**Implementación:**
- Los datos y nulos se mantienen en arrays separados para evitar boxing con `Nullable<T>`.
- `BitArray` en lugar de `bool[]` para la máscara de nulos ahorra un factor 8 de memoria.
- `AsSpan()` expone el array interno como `ReadOnlySpan<T>` para operaciones vectorizadas de alto rendimiento.

```csharp
// Construcción
var precios    = new DataColumn<double>("precio",   new double[] { 10.5, 20.0, 15.75 });
var cantidades = new DataColumn<int>("cantidad", 100);  // 100 filas inicializadas a null

// Acceso con semántica nullable (devuelve T? — null si la celda es nula)
double? p = precios[0];    // 10.5
int?    c = cantidades[5]; // null

// Asignación
precios[1]    = 25.0;
cantidades[5] = null;  // marca la celda como nula

// Acceso raw sin overhead (para hot-paths tras verificar IsNull manualmente)
double rawVal = precios.GetRawValue(0);

// Comparaciones — devuelven bool[] para usar con DataFrame.Where()
bool[] caros   = precios.GreaterThan(15.0);
bool[] baratos = precios.LessThan(12.0);
bool[] exactos = precios.EqualTo(20.0);
bool[] noNulos = precios.NotNullMask();

// Vista como span (para VectorOps internamente)
ReadOnlySpan<double> span = precios.AsSpan();
```

---

### StringColumn

Columna especializada para strings. A diferencia de `DataColumn<T>`, no usa `BitArray` para los nulos: `null` en el array ya tiene semántica de celda nula, ya que `string` es un tipo referencia.

**Implementación:**
- Un único `string[]` interno.
- `string.Empty` **no** es nulo (igual que pandas); solo `null` lo es.
- El constructor clona el array de entrada de forma defensiva para no compartir referencias.

```csharp
var ciudades = new StringColumn("ciudad", new[] { "Madrid", "Barcelona", null, "Sevilla" });

// Acceso
string c = ciudades[0];  // "Madrid"
string n = ciudades[2];  // null (celda nula)

// Modificación
ciudades[2] = "Valencia";
ciudades[3] = null;       // marcar como nula

// Comprobación de nulos
bool esNulo = ciudades.IsNull(2);  // true

// Máscaras de filtrado
bool[] esMadrid = ciudades.EqualsMask("Madrid");
bool[] isIn     = ciudades.IsInMask("Madrid", "Barcelona");

// Iteración
foreach (string ciudad in ciudades)
    Console.WriteLine(ciudad ?? "(nulo)");
```

---

### CategoricalColumn

Columna optimizada para strings con **baja cardinalidad** (pocas categorías únicas, muchas repeticiones). Internamente almacena un `int[]` de códigos y una lista de categorías, de forma análoga al tipo `category` de pandas.

**Implementación:**
- `_codes: int[]` — un entero por fila. `-1` representa nulo.
- `_categories: List<string>` — las categorías únicas en orden de primera aparición.
- `_categoryToCode: Dictionary<string, int>` — índice inverso para búsquedas O(1).
- Las comparaciones operan sobre enteros (los códigos), no sobre strings: significativamente más rápido en columnas con millones de filas.
- Las categorías con 0 ocurrencias tras un filtrado se **preservan** (igual que pandas), para que merge/join entre columnas del mismo dominio funcionen correctamente.

```csharp
var paises = new CategoricalColumn("pais", new[] { "ES", "FR", "ES", "DE", "FR", "ES" });

// Acceso
string p = paises[0];  // "ES"

// Filtrado eficiente
bool[] esSpain = paises.EqualsMask("ES");
bool[] iberia  = paises.IsInMask("ES", "PT");

// Acceso a código entero (bajo nivel)
int    code = paises.GetCode(0);        // 0
string str  = paises.DecodeCategory(0); // "ES"

// Búsqueda de código sin excepción
if (paises.TryGetCode("ES", out int c))
    Console.WriteLine($"Código de ES: {c}");
```

---

## DataFrame

El `DataFrame` es el contenedor principal. Internamente usa un `Dictionary<string, BaseColumn>` (acceso por nombre, insensible a mayúsculas) y una `List<string>` para preservar el orden de inserción.

### Creación

```csharp
// Desde columnas ya construidas
var df = DataFrame.FromColumns(
    new DataColumn<double>("precio",  new double[] { 10.5, 20.0, 15.75 }),
    new DataColumn<int>("ventas",     new int[]    { 120,  340,  85    }),
    new StringColumn("ciudad",        new string[] { "Madrid", "Barcelona", "Sevilla" })
);

// Añadir columna a un DataFrame existente
df.AddColumn(new DataColumn<bool>("activo", new bool[] { true, true, false }));

// Eliminar columna
df.TryRemoveColumn("activo");

// Propiedades
Console.WriteLine(df.RowCount);    // 3
Console.WriteLine(df.ColumnCount); // 3
```

---

### Acceso a columnas

```csharp
// Por nombre (devuelve BaseColumn)
BaseColumn col = df["precio"];

// Tipado — lanza InvalidCastException si el tipo no coincide
DataColumn<double> precios  = df.GetColumn<double>("precio");
StringColumn       ciudades = df.GetStringColumn("ciudad");

// Comprobar existencia
bool existe = df.ContainsColumn("precio");  // true

// Iterar columnas
foreach (string nombre in df.ColumnNames)
    Console.WriteLine($"{nombre}: {df[nombre].Length} filas");
```

---

### Filtrado

El filtrado se basa en **máscaras booleanas** (`bool[]`). Cada columna expone métodos para generar máscaras, y `MaskOps` permite combinarlas.

```csharp
DataColumn<double> precio = df.GetColumn<double>("precio");
StringColumn       ciudad = df.GetStringColumn("ciudad");

// Máscaras simples
bool[] caros   = precio.GreaterThan(15.0);
bool[] eMadrid = ciudad.EqualsMask("Madrid");

// Combinar con MaskOps
bool[] filtro = MaskOps.And(caros, eMadrid);

// Aplicar — devuelve un nuevo DataFrame (inmutable)
DataFrame resultado = df.Where(filtro);
```

---

### Head y Tail

```csharp
DataFrame primeras = df.Head(5);  // primeras 5 filas (por defecto)
DataFrame ultimas  = df.Tail(3);  // últimas 3 filas
```

---

### Print — Display tabular

`Print()` muestra el `DataFrame` en consola con formato tabular estilo pandas. Está implementado en `DataFramePrint.cs` implementa `DataFrameExtensions`, una clase estática de extensión. Esto permite añadir Print() a DataFrame sin modificar `DataFrame.cs` ni usar partial class.

**Ejemplo de salida:**

```
            ciudad        precio    ventas
       ─────────────────────────────────────
     0  Madrid         10.50       120
     1  Barcelona      20.00       340
     2  Sevilla          NaN        85
     3  Valencia       15.75       210

[4 rows × 3 columns]

dtypes:
  ciudad: category
  precio: float64
  ventas: int32
```

**Comportamiento:**
- Columnas **numéricas** alineadas a la derecha; texto a la izquierda (igual que pandas).
- Celdas nulas se muestran como `NaN`.
- Si el DataFrame supera `maxRows`, se muestran cabeza y cola con `...` en medio.
- Los valores más largos que `maxColWidth` se truncan con `…`.
- Línea de `dtypes` al final con el tipo real de cada columna.

**Mapeo de tipos a dtypes:**

| Tipo .NET              | dtype mostrado |
|------------------------|----------------|
| `DataColumn<double>`   | `float64`      |
| `DataColumn<float>`    | `float32`      |
| `DataColumn<int>`      | `int32`        |
| `DataColumn<long>`     | `int64`        |
| `DataColumn<bool>`     | `bool`         |
| `DataColumn<DateTime>` | `datetime64`   |
| `CategoricalColumn`    | `category`     |
| `StringColumn`         | `object`       |

```csharp
// Uso básico (10 filas, ancho máximo 12 caracteres por celda)
df.Print();

// Mostrar más filas
df.Print(maxRows: 20);

// Mostrar todas las filas sin truncar
df.Print(maxRows: 0);

// Columnas más anchas (útil para strings largos)
df.Print(maxColWidth: 25);

// Obtener la representación como string (para logging o tests)
string repr = df.ToDisplayString(maxRows: 5);

// Escribir a cualquier TextWriter
df.Print(writer: Console.Error, maxRows: 10);
```

**Ejemplo con truncado** (DataFrame de 100 filas):

```
            pais    ventas    precio
       ─────────────────────────────
     0   España    120.00     10.50
     1   Francia   340.00     20.00
     2  Alemania    85.00     15.75
   ...      ...       ...       ...
    97   España    210.00     18.00
    98   Italia    175.00     22.50
    99   Francia   300.00     19.00

[100 rows × 3 columns]

dtypes:
  pais: category
  ventas: float64
  precio: float64
```

> **Nota de implementación:** `DataFramePrint.cs` usa `partial class DataFrame` para dividir la clase en dos archivos. El compilador los fusiona en una sola clase al compilar, como si fueran uno. Esto permite mantener la lógica de display aislada sin necesidad de clases auxiliares ni métodos de extensión.

---

## Operaciones sobre máscaras — MaskOps

`MaskOps` proporciona operaciones lógicas element-wise sobre `bool[]`.

```csharp
using MiniPandas.Core.Operations.Math;

bool[] maskA = precioCol.GreaterThan(100);
bool[] maskB = ciudadCol.EqualsMask("Madrid");

// AND — true solo si ambas son true
bool[] y = MaskOps.And(maskA, maskB);

// OR — true si alguna es true
bool[] o = MaskOps.Or(maskA, maskB);

// NOT — invierte la máscara
bool[] no = MaskOps.Not(maskA);

// XOR — true si exactamente una es true
bool[] xo = MaskOps.Xor(maskA, maskB);

// Combinar muchas a la vez
bool[] todas  = MaskOps.All(maskA, maskB, maskC);  // equivale a A & B & C
bool[] alguna = MaskOps.Any(maskA, maskB, maskC);  // equivale a A | B | C

// Estadísticas sobre la máscara
int   nTrue   = MaskOps.CountTrue(mask);
int[] indices = MaskOps.TrueIndices(mask);  // posiciones donde es true
```

---

## Aritmética vectorizada — VectorOps

Operaciones aritméticas sobre columnas numéricas. Todas devuelven una nueva columna (inmutable). Los nulos se **propagan**: si cualquier operando es nulo, el resultado es nulo.

```csharp
using MiniPandas.Core.Operations.Math;

DataColumn<double> a = df.GetColumn<double>("precio");
DataColumn<double> b = df.GetColumn<double>("coste");

// ── Columna OP Columna ─────────────────────────────────────────────────────
DataColumn<double> suma     = VectorOps.Add(a, b);       // nombre: "precio+coste"
DataColumn<double> resta    = VectorOps.Subtract(a, b);
DataColumn<double> producto = VectorOps.Multiply(a, b);
DataColumn<double> cociente = VectorOps.Divide(a, b);    // div/0 → null (no excepción)

// ── Columna OP Escalar ─────────────────────────────────────────────────────
DataColumn<double> conIVA   = VectorOps.Multiply(a, 1.21);
DataColumn<double> ajustada = VectorOps.Add(a, 5.0);
DataColumn<double> normaliz = VectorOps.Divide(a, 100.0);

// ── Operaciones int ────────────────────────────────────────────────────────
DataColumn<int>    cantA   = df.GetColumn<int>("cantidad");
DataColumn<int>    cantB   = df.GetColumn<int>("devueltos");
DataColumn<int>    sumaInt = VectorOps.Add(cantA, cantB);
DataColumn<double> divInt  = VectorOps.Divide(cantA, cantB); // siempre devuelve double

// ── Estadísticas escalares ─────────────────────────────────────────────────
double sumaVal = VectorOps.Sum(a);    // NaN si todos null
double media   = VectorOps.Mean(a);
double min     = VectorOps.Min(a);
double max     = VectorOps.Max(a);
double std     = VectorOps.Std(a);   // desviación estándar muestral (n-1)
int    conteo  = VectorOps.Count(a); // no nulos

// Añadir la columna calculada al DataFrame
df.AddColumn(suma);
```

---

## GroupBy

`df.GroupBy(params string[] keys)` devuelve un `GroupByContext`, que encapsula los grupos sin materializarlos. La clave de grupo es la concatenación de los valores de las columnas clave separados por `"|"` (configurable con `GroupByOptions`).

```csharp
GroupByContext grp      = df.GroupBy("pais");
GroupByContext grpMulti = df.GroupBy("pais", "ciudad");

// Propiedades de inspección
int numGrupos                    = grp.GroupCount;
Dictionary<string, int> tamaños = grp.GroupSizes(); // clave → nº filas
```

**Separador de clave personalizado** (cuando los datos pueden contener `"|"`):

```csharp
var opts = new GroupByOptions(keySeparator: "\x00|\x00");
GroupByContext grp = df.GroupBy(opts, "categoria", "subcategoria");
```

---

### Agg

Agrega columnas numéricas por grupo. Devuelve un `DataFrame` con una fila por grupo.

`AggFunc` disponibles: `Sum`, `Mean`, `Min`, `Max`, `Count`, `CountUnique`, `Std`, `Var`, `Prod`, `Median`, `First`, `Last`.

```csharp
DataFrame resultado = df.GroupBy("pais").Agg(new Dictionary<string, AggFunc>
{
    { "ventas",   AggFunc.Sum   },
    { "precio",   AggFunc.Mean  },
    { "clientes", AggFunc.Count }
});
// resultado tiene columnas: "pais", "ventas", "precio", "clientes"

resultado.Print();
```

---

### Count

Equivalente a `pandas.groupby().size()`. Devuelve una columna `count` con el número de filas de cada grupo.

```csharp
DataFrame conteo = df.GroupBy("pais").Count();
// columnas: "pais", "count"
```

---

### Filter

Filtra grupos enteros según un predicado. El predicado recibe el sub-`DataFrame` del grupo. Internamente usa `GatherRows()` por grupo — O(tamaño_grupo) — en lugar de construir una máscara global O(n).

```csharp
// Solo grupos con más de 1000 ventas totales
DataFrame grandes = df.GroupBy("pais").Filter(grupo =>
{
    var ventas = grupo.GetColumn<double>("ventas");
    return VectorOps.Sum(ventas) > 1000;
});
```

---

### Transform

Calcula un agregado por grupo y lo **repite en cada fila** del grupo. El resultado tiene el mismo número de filas que el DataFrame original.

```csharp
// Añadir columna con la media del grupo para cada fila
BaseColumn mediaGrupo = df.GroupBy("pais").Transform("ventas", AggFunc.Mean);
df.AddColumn(mediaGrupo);  // nombre: "ventas_mean"
```

---

### Apply

Aplica una función arbitraria a cada grupo y concatena los resultados verticalmente.

```csharp
DataFrame resultado = df.GroupBy("pais").Apply(grupo =>
{
    var ventas = grupo.GetColumn<double>("ventas");
    double max = VectorOps.Max(ventas);
    DataColumn<double> normalizadas = VectorOps.Divide(ventas, max);
    grupo.AddColumn(normalizadas);
    return grupo;
});
```

> **Nota:** `Apply` es flexible pero más lento que `Agg`. Úsalo solo cuando la transformación no encaje en las funciones estándar.

---

## Merge (Join)

Implementa un **hash join** entre dos DataFrames. Equivale a `pandas.DataFrame.merge()`.

**Algoritmo:**
1. **Build:** indexa el DataFrame derecho en un `Dictionary<RowKey, List<int>>`.
2. **Probe:** itera el izquierdo buscando coincidencias.
3. **Gather:** construye las columnas del resultado via `ColumnGather`.

| `JoinType` | Comportamiento |
|---|---|
| `Inner` | Solo filas con coincidencia en ambos lados |
| `Left`  | Todas las filas del izquierdo; nulos si no hay coincidencia derecha |
| `Right` | Todas las filas del derecho; nulos si no hay coincidencia izquierda |
| `Outer` | Todas las filas de ambos lados |

```csharp
// Inner join (por defecto)
DataFrame resultado = left.Merge(right, on: "id");

// Left join
DataFrame resultado = left.Merge(right, on: "id", how: JoinType.Left);

// Join con columnas de distinto nombre
DataFrame resultado = left.Merge(right,
    leftOn:  new[] { "id_cliente" },
    rightOn: new[] { "cliente_id" },
    how: JoinType.Left);

// Join por múltiples columnas
DataFrame resultado = left.Merge(right,
    on:  new[] { "pais", "ciudad" },
    how: JoinType.Inner);
```

---

## IO — Carga y exportación

Todos los loaders implementan `IDataLoader`, lo que permite inyección de dependencias y sustitución por loaders falsos en tests.

```csharp
public interface IDataLoader
{
    DataFrame Load(string path, LoadOptions options = null);
}

// Uso genérico (el llamador no conoce el formato):
IDataLoader loader = new CsvLoader();
DataFrame df = loader.Load("datos.csv");
```

---

### CSV

**CsvLoader** implementa un parser RFC 4180 propio (sin dependencias externas):
- Campos entre comillas pueden contener el separador y saltos de línea.
- `""` dentro de un campo entrecomillado = comilla literal.
- BOM UTF-8 se ignora automáticamente.

```csharp
// Coma (por defecto)
DataFrame df = new CsvLoader().Load("datos.csv");

// Punto y coma (ficheros europeos)
DataFrame df = new CsvLoader().Load("datos.csv", CsvOptions.European);

// Opciones a medida
var opts = new CsvOptions(
    delimiter: '\t',             // tabulador
    hasHeader: true,
    categoricalThreshold: 0.3    // columnas con < 30% valores únicos → CategoricalColumn
);
DataFrame df = new CsvLoader().Load("datos.tsv", opts);

// Exportar
CsvExporter.Export(df, "salida.csv");
CsvExporter.Export(df, "salida.csv", delimiter: ';');
```

---

### Excel

**ExcelLoader** usa la librería `ExcelDataReader`. Soporta `.xlsx` y `.xls`. Las filas completamente vacías se descartan.

```csharp
DataFrame df = new ExcelLoader().Load("datos.xlsx");

DataFrame df = new ExcelLoader().Load("datos.xlsx", new LoadOptions
{
    HasHeader = true,
    CategoricalThreshold = 0.5
});

// Exportar (genera .xlsx sin dependencias externas, usando Open XML puro)
ExcelExporter.Export(df, "salida.xlsx");
ExcelExporter.Export(df, "salida.xlsx", sheetName: "Ventas");
```

---

### JSON

**JsonLoader** soporta tres orientaciones. Si la orientación no se especifica, la detecta automáticamente inspeccionando el token raíz del JSON.

| Orientación | Estructura | Uso típico |
|---|---|---|
| `Records` | `[{col:val}, ...]`               | Legible, compatible con pandas |
| `Columns` | `{col:[val,...]}`                | Compacto, ideal para gráficos  |
| `Split`   | `{columns:[...], data:[[],...]}` | Más compacto, SQL bulk         |

**Autodetección:**
- Token raíz `[` → Records
- Token raíz `{` con claves `"columns"` y `"data"` → Split
- Token raíz `{` en cualquier otro caso → Columns

```csharp
// Autodetección (recomendado para lectura)
DataFrame df = new JsonLoader().Load("datos.json");

// Orientación explícita
DataFrame df = new JsonLoader().Load("datos.json", JsonOptions.Records);
DataFrame df = new JsonLoader().Load("datos.json", new JsonOptions(JsonOrientation.Split));

// Exportar
JsonExporter.Export(df, "salida.json");                               // Records, indentado
JsonExporter.Export(df, "salida_columns.json", JsonOptions.Columns);
JsonExporter.Export(df, "salida_compact.json",
    new JsonOptions(JsonOrientation.Records, indented: false));
```

---

### SchemaInference

`SchemaInference` infiere el tipo de cada columna a partir de datos crudos (`object[][]`). Lo usan internamente todos los loaders. La cadena de inferencia aplica los inferrers en orden de prioridad (más restrictivo primero):

1. `IntInferrer` — enteros puros (`int`, `long` o strings numéricos enteros).
2. `DoubleInferrer` — numérico con decimales.
3. `DateTimeInferrer` — fechas nativas o strings parseables como fecha.
4. Fallback a `StringColumn` o `CategoricalColumn` según `CategoricalThreshold`.

El umbral `CategoricalThreshold` (entre 0 y 1) controla cuándo una columna de strings se convierte en `CategoricalColumn`. Si `valores_únicos / total_filas < threshold`, la columna se almacena como categórica.

```csharp
// Usar directamente en un loader personalizado
var columnas = SchemaInference.InferColumns(
    names:   new[] { "ciudad", "precio", "fecha" },
    rawRows: rawRows,
    options: new LoadOptions { CategoricalThreshold = 0.4 }
);

var df = new DataFrame(filas);
foreach (var col in columnas)
    df.AddColumn(col);
```

---

## Ejemplo completo

```csharp
using MiniPandas.Core;
using MiniPandas.Core.Columns;
using MiniPandas.Core.IO.Csv;
using MiniPandas.Core.IO.Json;
using MiniPandas.Core.Operations.GroupBy;
using MiniPandas.Core.Operations.Math;
using MiniPandas.Core.Operations.Merge;

// 1. Cargar datos
DataFrame ventas = new CsvLoader().Load("ventas.csv");

// 2. Inspeccionar el DataFrame
ventas.Print();

// 3. Filtrar: pedidos con precio > 50 y país = "España"
DataColumn<double> precio = ventas.GetColumn<double>("precio");
StringColumn       pais   = ventas.GetStringColumn("pais");

DataFrame filtrado = ventas.Where(MaskOps.And(
    precio.GreaterThan(50),
    pais.EqualsMask("España")
));
filtrado.Print(maxRows: 5);

// 4. Calcular columna derivada
DataColumn<double> coste  = filtrado.GetColumn<double>("coste");
DataColumn<double> margen = VectorOps.Subtract(precio, coste);
filtrado.AddColumn(margen);

// 5. Agrupar y agregar
DataFrame resumen = filtrado.GroupBy("pais").Agg(
    new Dictionary<string, AggFunc>
    {
        { "precio", AggFunc.Mean  },
        { "ventas", AggFunc.Sum   },
        { "pais",   AggFunc.Count }
    });
resumen.Print();

// 6. Merge con tabla de clientes
DataFrame clientes    = new CsvLoader().Load("clientes.csv");
DataFrame enriquecido = filtrado.Merge(clientes,
    leftOn:  new[] { "id_cliente" },
    rightOn: new[] { "cliente_id" },
    how: JoinType.Left);

// 7. Exportar
JsonExporter.Export(resumen,      "resumen_pais.json",      JsonOptions.Records);
CsvExporter.Export(enriquecido,   "ventas_enriquecidas.csv");
```

---

## Dependencias

| Paquete | Versión | Uso |
|---|---|---|
| `ExcelDataReader`         | ≥ 3.6  | Lectura de `.xlsx` / `.xls`           |
| `ExcelDataReader.DataSet` | ≥ 3.6  | Adapter DataSet para ExcelDataReader  |
| `Newtonsoft.Json`         | ≥ 13.0 | Parsing y serialización JSON          |

El exportador Excel (`ExcelExporter`) no tiene dependencias externas: genera `.xlsx` directamente con Open XML puro usando `System.IO.Compression`.
