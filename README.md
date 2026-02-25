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
- [Dependencias](#dependencias)

---

## Arquitectura general

```
MiniPandas.Core
├── Columns/
│   ├── BaseColumn.cs          ← contrato abstracto de columna
│   ├── DataColumn.cs          ← columna tipada para tipos valor (double, int, DateTime…)
│   ├── StringColumn.cs        ← columna de strings con null nativo
│   └── CategoricalColumn.cs   ← columna categórica con codificación por enteros
├── DataFrame.cs               ← contenedor principal de columnas
├── IO/
│   ├── IDataLoader.cs         ← interfaz genérica de loaders
│   ├── LoadOptions.cs         ← opciones base compartidas
│   ├── Core/SchemaInference.cs← inferencia automática de tipos
│   ├── Csv/                   ← CsvLoader, CsvExporter, CsvOptions
│   ├── Excel/                 ← ExcelLoader, ExcelExporter
│   └── Json/                  ← JsonLoader, JsonExporter, JsonOptions
└── Operations/
    ├── Math/
    │   ├── MaskOps.cs         ← operaciones lógicas sobre bool[]
    │   └── VectorOps.cs       ← aritmética vectorizada sobre columnas
    ├── GroupBy/
    │   ├── GroupByContext.cs  ← resultado de df.GroupBy(...)
    │   ├── Aggregations.cs    ← implementaciones de AggFunc
    │   ├── AggFunc.cs         ← enum de funciones de agregación
    │   ├── ColumnGather.cs    ← recolección de filas por índice sin boxing
    │   └── GroupByOptions.cs  ← configuración del separador de clave
    └── Merge/
        ├── MergeOp.cs         ← hash join entre dos DataFrames
        └── JoinType.cs        ← enum Inner / Left / Right / Outer
```

El flujo típico es: **cargar datos → construir DataFrame → filtrar/transformar → exportar**.

---

## Columnas

### BaseColumn

`BaseColumn` es la clase abstracta que todos los tipos de columna implementan. Define el contrato mínimo para ser almacenada en un `DataFrame`.

**Miembros abstractos:**

| Miembro | Descripción |
|---|---|
| `string Name` | Nombre de la columna |
| `int Length` | Número de filas |
| `bool IsNull(int index)` | ¿Es nula la celda? |
| `object GetBoxed(int index)` | Devuelve el valor como `object` (boxing) |
| `BaseColumn Filter(bool[] mask)` | Devuelve nueva columna con las filas `true` |

El boxing de `GetBoxed` está pensado solo para operaciones genéricas (display, JSON, comparaciones heterogéneas). En hot-paths usa siempre el tipo concreto.

---

### DataColumn\<T\>

Columna tipada para **tipos valor**: `double`, `int`, `DateTime`, `bool`, etc. La restricción `where T : struct, IComparable<T>` permite nulos mediante `T?` (Nullable) y habilita comparaciones sin conocer el tipo concreto.

Los nulos se representan internamente con un `BitArray` paralelo al array de datos.

**Constructores:**

```csharp
// Columna vacía de N filas (todas nulas por defecto)
var col = new DataColumn<double>("precio", 100);

// Desde array de datos (sin nulos)
var col = new DataColumn<double>("precio", new double[] { 1.5, 2.3, 3.7 });

// Desde array con máscara de nulos
var data  = new double[] { 1.5, 0.0, 3.7 };
var nulls = new bool[]   { false, true, false }; // la segunda celda es nula
var col = new DataColumn<double>("precio", data, nulls);
```

**Acceso con semántica nullable:**

```csharp
DataColumn<double> col = df.GetColumn<double>("precio");

double? valor = col[0];          // null si la celda es nula
if (valor.HasValue)
    Console.WriteLine(valor.Value);

// En hot-paths: verificar nulo primero, luego acceder sin overhead
if (!col.IsNull(i))
    double raw = col.GetRawValue(i);
```

**Comparaciones (devuelven `bool[]` para usar con `Where`):**

```csharp
bool[] mask = col.GreaterThan(100.0);
bool[] mask = col.LessThan(50.0);
bool[] mask = col.EqualTo(42.0);
bool[] mask = col.IsNull();       // celdas nulas
bool[] mask = col.IsNotNull();    // celdas con valor
```

**Vista raw para operaciones vectorizadas:**

```csharp
ReadOnlySpan<double> span = col.AsSpan();
```

---

### StringColumn

Columna especializada para `string`. No usa `DataColumn<T>` porque `string` es un tipo referencia: el propio `null` del array representa la celda nula, sin necesitar `BitArray`.

```csharp
// Desde array (null en el array = celda nula; string.Empty != null)
var col = new StringColumn("ciudad", new string[] { "Madrid", null, "Barcelona" });

// Acceso
string ciudad = col[0];   // "Madrid"
bool esNula   = col.IsNull(1);  // true

// Asignación
col[2] = null;  // marca la celda como nula

// Filtros útiles
bool[] mask    = col.EqualsMask("Madrid");                        // igualdad
bool[] maskIn  = col.IsInMask("Madrid", "Barcelona", "Valencia"); // isin
```

**Conversión a CategoricalColumn:**

```csharp
// Útil cuando CardinalityRatio < 0.5 (muchos valores repetidos)
var catCol = CategoricalColumn.FromStringColumn(col);
```

---

### CategoricalColumn

Almacena strings como **códigos enteros** (`int[]`) más un diccionario `string → int`. Es más eficiente en memoria y operaciones de agrupación cuando la cardinalidad es baja (pocos valores únicos repetidos muchas veces).

- Código `-1` = celda nula.
- Las categorías se asignan en **orden de primera aparición**.
- El diccionario se reutiliza en `Filter` y `GatherByIndices` sin recalcular (O(1)).

```csharp
var col = new CategoricalColumn("pais", new string[]
{
    "España", "Francia", "España", null, "Francia"
});

Console.WriteLine(col.CategoryCount);        // 2
Console.WriteLine(col.Categories[0]);        // "España"
Console.WriteLine(col[2]);                   // "España"
Console.WriteLine(col.IsNull(3));            // true

// Acceso al código entero (para operaciones de bajo nivel)
int code = col.GetCode(0);                   // 0

// Decodificar código → string
string str = col.DecodeCategory(code);       // "España"

// Volver a StringColumn (O(n))
StringColumn strCol = col.ToStringColumn();
```

---

## DataFrame

El `DataFrame` es el contenedor principal. Internamente usa un `Dictionary<string, BaseColumn>` (acceso por nombre, insensible a mayúsculas) y una `List<string>` para preservar el orden de inserción.

### Creación

**Desde columnas ya construidas:**

```csharp
var precios  = new DataColumn<double>("precio", new double[] { 10.5, 20.0, 15.3 });
var ciudades = new StringColumn("ciudad", new string[] { "Madrid", "Barcelona", "Sevilla" });

// Con IEnumerable<BaseColumn>
var df = DataFrame.FromColumns(new BaseColumn[] { precios, ciudades });

// Con params (más cómodo)
var df = DataFrame.FromColumns(precios, ciudades);
```

**DataFrame vacío de N filas (para añadir columnas después):**

```csharp
var df = new DataFrame(100);
df.AddColumn(new DataColumn<double>("precio", 100));
```

### Acceso a columnas

```csharp
// Por nombre (insensible a mayúsculas)
BaseColumn col = df["precio"];

// Por índice de posición
BaseColumn col = df[0];

// Acceso tipado — lanza InvalidCastException si el tipo no coincide
DataColumn<double>  colD = df.GetColumn<double>("precio");
StringColumn        colS = df.GetStringColumn("ciudad");

// Información general
int filas    = df.RowCount;
int columnas = df.ColumnCount;
IEnumerable<string> nombres = df.ColumnNames;

// Comprobación
bool existe = df.ContainsColumn("precio");

// Eliminar columna
bool ok = df.TryRemoveColumn("precio");
```

### Filtrado

`Where(bool[] mask)` devuelve un nuevo `DataFrame` con las filas donde la máscara es `true`. La máscara debe tener la misma longitud que `RowCount`.

```csharp
// Filtrar por precio > 100
DataColumn<double> precio = df.GetColumn<double>("precio");
bool[] mask = precio.GreaterThan(100.0);
DataFrame resultado = df.Where(mask);

// Combinar condiciones
StringColumn ciudad = df.GetStringColumn("ciudad");
bool[] maskCiudad = ciudad.EqualsMask("Madrid");
bool[] maskCombinada = MaskOps.And(mask, maskCiudad);
DataFrame filtrado = df.Where(maskCombinada);
```

### Head y Tail

```csharp
DataFrame primeras5 = df.Head();     // primeras 5 filas (por defecto)
DataFrame primeras10 = df.Head(10);

DataFrame ultimas5 = df.Tail();      // últimas 5 filas
DataFrame ultimas3 = df.Tail(3);
```

---

## Operaciones sobre máscaras — MaskOps

Las máscaras (`bool[]`) son el puente entre comparaciones y filtrado. `MaskOps` proporciona operaciones lógicas element-wise.

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
bool[] todas = MaskOps.All(maskA, maskB, maskC);  // equivale a A & B & C
bool[] alguna = MaskOps.Any(maskA, maskB, maskC); // equivale a A | B | C

// Estadísticas sobre la máscara
int nTrue     = MaskOps.CountTrue(mask);
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
DataColumn<double> suma      = VectorOps.Add(a, b);       // nombre: "precio+coste"
DataColumn<double> resta     = VectorOps.Subtract(a, b);
DataColumn<double> producto  = VectorOps.Multiply(a, b);
DataColumn<double> cociente  = VectorOps.Divide(a, b);    // div/0 → null (no excepción)

// ── Columna OP Escalar ─────────────────────────────────────────────────────
DataColumn<double> conIVA    = VectorOps.Multiply(a, 1.21);
DataColumn<double> ajustada  = VectorOps.Add(a, 5.0);
DataColumn<double> reducida  = VectorOps.Subtract(a, 10.0);
DataColumn<double> normaliz  = VectorOps.Divide(a, 100.0); // escalar 0 → DivideByZeroException

// ── Operaciones int ────────────────────────────────────────────────────────
DataColumn<int> cantA = df.GetColumn<int>("cantidad");
DataColumn<int> cantB = df.GetColumn<int>("devueltos");
DataColumn<int>    sumaInt  = VectorOps.Add(cantA, cantB);
DataColumn<double> divInt   = VectorOps.Divide(cantA, cantB); // siempre double

// ── Estadísticas escalares ─────────────────────────────────────────────────
double suma_val  = VectorOps.Sum(a);    // NaN si todos null
double media     = VectorOps.Mean(a);
double min       = VectorOps.Min(a);
double max       = VectorOps.Max(a);
double std       = VectorOps.Std(a);    // desviación estándar muestral (n-1)
int    conteo    = VectorOps.Count(a);  // no nulos

// Añadir la columna calculada al DataFrame original
df.AddColumn(suma);
```

---

## GroupBy

`df.GroupBy(params string[] keys)` devuelve un `GroupByContext`, que encapsula los grupos sin materializarlos. La clave de grupo es la concatenación de los valores de las columnas clave separados por `"|"` (configurable con `GroupByOptions`).

```csharp
GroupByContext grp = df.GroupBy("pais");
GroupByContext grpMulti = df.GroupBy("pais", "ciudad");

// Propiedades de inspección
int numGrupos = grp.GroupCount;
Dictionary<string, int> tamaños = grp.GroupSizes(); // clave → nº filas
```

**Separador de clave personalizado** (cuando los datos pueden contener `"|"`):

```csharp
var opts = new GroupByOptions(keySeparator: "\x00|\x00");
GroupByContext grp = df.GroupBy(opts, "categoria", "subcategoria");
```

### Agg

Agrega columnas numéricas por grupo. Devuelve un `DataFrame` con una fila por grupo.

`AggFunc` disponibles: `Sum`, `Mean`, `Min`, `Max`, `Count`, `CountUnique`, `Std`, `Var`, `Prod`, `Median`, `First`, `Last`.

```csharp
DataFrame resultado = df.GroupBy("pais").Agg(new Dictionary<string, AggFunc>
{
    { "ventas",   AggFunc.Sum  },
    { "precio",   AggFunc.Mean },
    { "clientes", AggFunc.Count }
});

// resultado tiene columnas: "pais", "ventas", "precio", "clientes"
```

### Count

Equivalente a `pandas.groupby().size()`. Devuelve una columna `count` con el número de filas de cada grupo.

```csharp
DataFrame conteo = df.GroupBy("pais").Count();
// columnas: "pais", "count"
```

### Filter

Filtra grupos enteros según un predicado. El predicado recibe el sub-`DataFrame` del grupo.

```csharp
// Solo grupos con más de 1000 ventas totales
DataFrame grandes = df.GroupBy("pais").Filter(grupo =>
{
    var ventas = grupo.GetColumn<double>("ventas");
    return VectorOps.Sum(ventas) > 1000;
});
```

La implementación usa `GatherRows()` por grupo (O(tamaño_grupo)) en lugar de construir una máscara global (O(n)).

### Transform

Calcula un agregado por grupo y lo **repite en cada fila** del grupo. El resultado tiene el mismo número de filas que el DataFrame original.

```csharp
// Añadir columna con la media del grupo para cada fila
BaseColumn mediaGrupo = df.GroupBy("pais").Transform("ventas", AggFunc.Mean);
df.AddColumn(mediaGrupo);  // nombre: "ventas_mean"
```

### Apply

Aplica una función arbitraria a cada grupo y concatena los resultados verticalmente.

```csharp
DataFrame resultado = df.GroupBy("pais").Apply(grupo =>
{
    // Normalizar ventas dentro del grupo
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

Las filas sin pareja (en joins externos) producen celdas nulas (índice `-1`).

```csharp
// Join por una columna con el mismo nombre en ambos
DataFrame resultado = left.Merge(right, "id_cliente");

// Join por una columna con nombres distintos
DataFrame resultado = left.Merge(right,
    leftOn:  new[] { "id_cliente" },
    rightOn: new[] { "cliente_id" });

// Join por múltiples columnas
DataFrame resultado = left.Merge(right,
    leftOn:  new[] { "pais", "ciudad" },
    rightOn: new[] { "pais", "ciudad" },
    how: JoinType.Left);
```

**Tipos de join:**

| `JoinType` | Equivalente SQL | Comportamiento |
|---|---|---|
| `Inner` | `INNER JOIN` | Solo filas con clave en ambos (por defecto) |
| `Left` | `LEFT JOIN` | Todas las filas del izquierdo; nulos en columnas derechas sin pareja |
| `Right` | `RIGHT JOIN` | Todas las filas del derecho |
| `Outer` | `FULL OUTER JOIN` | Todas las filas de ambos; nulos donde no hay pareja |

```csharp
// Ejemplo completo
var pedidos = DataFrame.FromColumns(
    new DataColumn<int>("id_pedido", new int[] { 1, 2, 3 }),
    new DataColumn<int>("id_cliente", new int[] { 10, 20, 10 }),
    new DataColumn<double>("importe", new double[] { 150.0, 200.0, 80.0 }));

var clientes = DataFrame.FromColumns(
    new DataColumn<int>("id_cliente", new int[] { 10, 20, 30 }),
    new StringColumn("nombre", new string[] { "Ana", "Luis", "Marta" }));

DataFrame joined = pedidos.Merge(clientes, "id_cliente");
// Resultado: id_pedido | id_cliente | importe | nombre
//                    1 |         10 |   150.0 | Ana
//                    2 |         20 |   200.0 | Luis
//                    3 |         10 |    80.0 | Ana
```

---

## IO — Carga y exportación

Todos los loaders implementan `IDataLoader` y aceptan opciones genéricas (`LoadOptions`) o específicas del formato.

```csharp
public interface IDataLoader
{
    DataFrame Load(string path, LoadOptions options = null);
}
```

Esto permite inyección de dependencias y tests con loaders falsos:

```csharp
IDataLoader loader = new CsvLoader();
DataFrame df = loader.Load("datos.csv");
```

### CSV

**CsvLoader** implementa un parser RFC 4180 propio (sin dependencias externas):
- Campos entre comillas pueden contener el separador y saltos de línea.
- `""` dentro de un campo entrecomillado = comilla literal.
- BOM UTF-8 se ignora automáticamente.

```csharp
// Carga con coma (por defecto)
DataFrame df = new CsvLoader().Load("datos.csv");

// Carga con punto y coma (ficheros europeos)
DataFrame df = new CsvLoader().Load("datos.csv", CsvOptions.European);

// Opciones a medida
var opts = new CsvOptions(
    delimiter: '\t',          // tabulador
    hasHeader: true,
    categoricalThreshold: 0.3 // columnas con < 30% valores únicos → CategoricalColumn
);
DataFrame df = new CsvLoader().Load("datos.tsv", opts);

// Exportar a CSV
CsvExporter.Export(df, "salida.csv");
CsvExporter.Export(df, "salida.csv", delimiter: ';');
```

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

### JSON

**JsonLoader** soporta tres orientaciones. Si la orientación no se especifica, la detecta automáticamente inspeccionando el token raíz del JSON.

| Orientación | Estructura | Uso típico |
|---|---|---|
| `Records` | `[{col:val}, ...]` | Legible, compatible con pandas |
| `Columns` | `{col:[val,...]}` | Compacto, ideal para gráficos |
| `Split` | `{columns:[...], data:[[],...]}` | Más compacto, SQL bulk |

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
JsonExporter.Export(df, "salida.json");                              // Records, indentado
JsonExporter.Export(df, "salida_columns.json", JsonOptions.Columns);
JsonExporter.Export(df, "salida_compact.json",
    new JsonOptions(JsonOrientation.Records, indented: false));
```

### SchemaInference

`SchemaInference` infiere el tipo de cada columna a partir de datos crudos (`object[][]`). Es utilizado internamente por todos los loaders y puede usarse directamente para escenarios personalizados.

**Cadena de inferencia (orden de prioridad):**
1. `IntInferrer` — enteros puros (sin parte decimal)
2. `DoubleInferrer` — numérico con decimales o strings numéricos
3. `DateTimeInferrer` — fechas nativas o strings parseables como fecha
4. Fallback a `StringColumn` o `CategoricalColumn` (según `CategoricalThreshold`)

El umbral categórico (por defecto `0.5`) determina si una columna de strings con alta repetición se convierte en `CategoricalColumn`:

```
CategoricalColumn si: (nº valores únicos / nº total filas) < CategoricalThreshold
```

```csharp
// Uso interno — los loaders lo invocan automáticamente
// Raramente necesitarás llamarlo directamente, pero es posible:
var opts = new LoadOptions { CategoricalThreshold = 0.3 };
IEnumerable<BaseColumn> cols = SchemaInference.InferColumns(names, rawRows, opts);
DataFrame df = DataFrame.FromColumns(cols);
```

---

## Dependencias

| Paquete | Versión | Uso |
|---|---|---|
| `ExcelDataReader` | 3.8.0 | Lectura de ficheros Excel (.xlsx/.xls) |
| `Newtonsoft.Json` | 13.0.4 | Lectura y escritura de JSON |
| `System.Memory` | 4.6.3 | `ReadOnlySpan<T>` en .NET 4.7.2 |
| `System.Buffers` | 4.6.1 | Soporte de memoria para System.Memory |
| `System.Numerics.Vectors` | 4.6.1 | Operaciones SIMD (soporte transitivo) |
| `System.ValueTuple` | 4.6.1 | Tuplas de valor en .NET 4.7.2 |
| `System.Runtime.CompilerServices.Unsafe` | 6.1.2 | Interoperabilidad de bajo nivel |
| `System.Text.Encoding.CodePages` | 10.0.3 | Codificaciones de texto extendidas |

**Restaurar paquetes NuGet:**

```bash
nuget restore MiniPandas.Core.csproj
```

**Compilar:**

```bash
msbuild MiniPandas.Core.csproj /p:Configuration=Release
```

---

## Ejemplo completo

```csharp
using MiniPandas.Core;
using MiniPandas.Core.Columns;
using MiniPandas.Core.IO.Csv;
using MiniPandas.Core.IO.Json;
using MiniPandas.Core.Operations.Math;
using MiniPandas.Core.Operations.Merge;
using System.Collections.Generic;

// 1. Cargar datos
DataFrame ventas = new CsvLoader().Load("ventas.csv");

// 2. Inspeccionar
Console.WriteLine($"Filas: {ventas.RowCount}, Columnas: {ventas.ColumnCount}");
DataFrame muestra = ventas.Head(5);

// 3. Filtrar
DataColumn<double> precio = ventas.GetColumn<double>("precio");
StringColumn pais = ventas.GetStringColumn("pais");

bool[] maskPrecio = precio.GreaterThan(50.0);
bool[] maskPais   = pais.IsInMask("España", "Francia");
bool[] maskFinal  = MaskOps.And(maskPrecio, maskPais);
DataFrame filtrado = ventas.Where(maskFinal);

// 4. Calcular columna derivada
DataColumn<double> coste = filtrado.GetColumn<double>("coste");
DataColumn<double> margen = VectorOps.Subtract(precio, coste);
filtrado.AddColumn(new DataColumn<double>("margen",
    // VectorOps devuelve la columna calculada; la renombramos
    ((DataColumn<double>)margen).AsSpan().ToArray()));

// 5. Agrupar y agregar
DataFrame resumenPais = filtrado.GroupBy("pais").Agg(
    new Dictionary<string, AggFunc>
    {
        { "precio", AggFunc.Mean  },
        { "ventas", AggFunc.Sum   },
        { "pais",   AggFunc.Count }  // nº pedidos por país
    });

// 6. Merge con tabla de clientes
DataFrame clientes = new CsvLoader().Load("clientes.csv");
DataFrame enriquecido = filtrado.Merge(clientes,
    leftOn:  new[] { "id_cliente" },
    rightOn: new[] { "cliente_id" },
    how: JoinType.Left);

// 7. Exportar
JsonExporter.Export(resumenPais, "resumen_pais.json", JsonOptions.Records);
CsvExporter.Export(enriquecido, "ventas_enriquecidas.csv");
```
