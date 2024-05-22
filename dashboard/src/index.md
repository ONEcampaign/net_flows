---
title: Net flows data
toc: false
theme: [light, wide]
sidebar: true
sql: 
  netFlows: ./data/full_flows_country.parquet
---


```js

import {yearSelector, pricesSelector, continentSelector, incomeSelector,counterPartTypeSelector, 
  counterPartSelector, countrySelector
} from "./components/filters.js";

import * as aq from "npm:arquero";

import {xlsxButton} from "./components/download.js"

```


# Net flows to developing countries

For this research, we use data from the [World Bank's International Debt Statistics (IDS)](https://databank.worldbank.org/source/international-debt-statistics) database and the OECD DAC [Creditor Reporting System (CRS)](https://stats.oecd.org/Index.aspx?DataSetCode=crs1) database.

<br>

## Explore the data
Use the available filters to explore the detailed data.

```sql id=[...data]
SELECT * FROM netFlows
```


<div>

```js
const selectYear =view(yearSelector({data:data}));
```

</div>

<div>

```js
const selectPrices = view(pricesSelector({data:data, selectYear: selectYear}));
```
</div>


<div>

```js
const selectContinent =view(continentSelector({data:data, selectYear: selectYear, selectPrices:selectPrices}));
```

</div>

<div>

```js
const selectIncome = view(incomeSelector({data:data, selectYear: selectYear,
  selectPrices:selectPrices, selectContinent:selectContinent
}));
```
</div>


<div>

```js
const selectCounterpartType =view(counterPartTypeSelector({data:data, selectYear: selectYear, 
selectPrices:selectPrices, selectContinent:selectContinent, selectIncome:selectIncome
}));
```

</div>

<div>

```js
const selectCounterpart = view(counterPartSelector({data:data, selectYear: selectYear,
  selectPrices:selectPrices, selectContinent:selectContinent, selectIncome:selectIncome, selectCounterpartType:selectCounterpartType
}));
```
</div>

<div>

```js
const selectCountry = view(countrySelector({data:data, selectYear: selectYear,
  selectPrices:selectPrices, selectContinent:selectContinent, selectIncome:selectIncome, selectCounterpartType:selectCounterpartType, selectCounterpart:selectCounterpart
}));
```
</div>

You can use the following parameters to view the data in different levels of detail.
To group as *net flows* you can uncheck `indicator_type`.


<div>

```js

const groupBy = view(Inputs.checkbox(
  [
    "year",
    "continent",
    "income_level",
    "country",
    "prices",
    "indicator",
    "indicator_type",
    "counterpart_type",
    "counterpart_area"
  ],
  {
    label: "Select variables to group by...",
    value: ["year","income_level", "indicator_type"]
  }
))

```
</div>


<div>

```js
const filteredData = data
        .filter((d) => selectYear.includes(d.year))
        .filter((d) => d.prices === selectPrices)
        .filter((d) => d.continent === selectContinent || selectContinent === "All")
        .filter((d) => selectIncome.includes(d.income_level))
        .filter((d) => selectCounterpartType.includes(d.counterpart_type ))
        .filter((d) => d.counterpart_area === selectCounterpart || selectCounterpart === "All")
        .filter((d) => d.country === selectCountry || selectCountry === 'All')
```

```js 
const dataTable = aq
  .from(filteredData)
  .groupby(groupBy)
  .rollup({ value: (d) => aq.op.sum(d.value) })
  // .orderby(groupBy.concat(["year", "indicator_type"]));
```

```js
let maxVal = d3.mean(dataTable, (d) => d["value"]);
const tablebn = maxVal > 1e9 ? true : false;
```

```js
const table = Inputs.table(
  dataTable,
  {
    width: {
      year: 50,
      continent: 80,
      indicator_type: 80
    },
    rows: 17,
    layout: "auto",
    header: {
      value: tablebn ? "Value (US $ billion)" : "Value (US $ million)",
      counterpart_area: "Counterpart",
      indicator_type: "Indicator Type"
    },
    format: {
      year: (x) => x.toFixed(0),
      value: (x) =>
        (tablebn ? x / 1e9 : x / 1e6).toLocaleString(undefined, {
          minimumFractionDigits: 0,
          maximumFractionDigits: tablebn ? 2 : 1
        })
    }
  }
)
```

<div class="card"> ${view(table)}</div>
<span>${xlsxButton(table)}</span>



---

## A note on the data

In terms of **inflows** we focus on:
- Debt disbursements of public and publicly guaranteed (PPG) long-term debt. This includes debt from bilateral, multilateral and private creditors.
- Grant disbursements (from providers of Official Development Assistance who report to the OECD DAC). 


In terms of **outflows** we focus on:
- Debt service payments (including both principal and interest payments)

In this context, **net flows** mean PPG debt and grant inflows minus debt service payments on PPG debt.

All debt is Public and Publicly Guaranteed long-term debt. In other words, "private" debt (for example) is debt public debt owed to private creditors.

<div class="note">
We only include countries with available data on inflows and outflows. We exclude Russia, China, and Ukraine from the list of debtors/recipients, given the significant resources flowing to and from these countries, related to conflict and geopolitics.
</div>

