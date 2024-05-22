import * as Inputs from "npm:@observablehq/inputs";
import * as d3 from "npm:d3";


export function unique(values) {
    return [...new Set(values)].sort(d3.ascending);
}

export function yearSelector({ data }) {
    return Inputs.checkbox(unique(data.map((d) => d.year)), {
        label: "Year(s)",
        format: (d) => d.toString(),
        value: [2022],
    });
}

export function pricesSelector({ data, selectYear }) {
    const year = selectYear;
    const filteredData = data.filter((d) => year.includes(d.year))


    const options = unique(filteredData.map((d) => d.prices))

    return Inputs.select(options, { label: "Prices", value: "current" });
}


export function continentSelector({ data, selectYear, selectPrices }) {
    const year = selectYear;
    const prices = selectPrices;
    const filteredData = data
        .filter((d) => year.includes(d.year))
        .filter((d) => d.prices === prices);

    const options = Array.from(["All"]).concat(
        unique(filteredData.map((d) => d.continent))
    );

    return Inputs.select(options, { label: "Continent" });
}


export function incomeSelector({ data, selectYear, selectPrices, selectContinent }) {
    const year = selectYear;
    const prices = selectPrices;
    const continent = selectContinent;

    const filteredData = data
        .filter((d) => year.includes(d.year))
        .filter((d) => d.prices === prices)
        .filter((d) => d.continent === continent || continent === "All");

    const options = unique(filteredData.map((d) => d.income_level));

    return Inputs.checkbox(options, { label: "Income Level", value:['Low income','Lower middle income','Upper middle income'] });
}

export function counterPartTypeSelector({ data, selectYear, selectPrices, selectContinent, selectIncome }) {
    const year = selectYear;
    const prices = selectPrices;
    const continent = selectContinent;
    const income = selectIncome;

    const filteredData = data
        .filter((d) => year.includes(d.year))
        .filter((d) => d.prices === prices)
        .filter((d) => d.continent === continent || continent === "All")
        .filter((d) => income.includes(d.income_level));

    const options = Array.from(["All"]).concat(
        unique(filteredData.map((d) => d.counterpart_type))
    );

    return Inputs.select(options, { label: "Counterpart Type" });
}

export function counterPartSelector({ data, selectYear, selectPrices, selectContinent, selectIncome, selectCounterpartType }) {
    const year = selectYear;
    const prices = selectPrices;
    const continent = selectContinent;
    const income = selectIncome;
    const counterpartType = selectCounterpartType;

    const filteredData = data
        .filter((d) => year.includes(d.year))
        .filter((d) => d.prices === prices)
        .filter((d) => d.continent === continent || continent === "All")
        .filter((d) => income.includes(d.income_level))
        .filter((d) => d.counterpart_type === counterpartType || counterpartType === "All");

    const options = Array.from(["All"]).concat(
        unique(filteredData.map((d) => d.counterpart_area))
    );

    return Inputs.select(options, { label: "Counterpart" });
}


export function countrySelector({
    data,
    selectYear,
    selectPrices,
    selectContinent,
    selectIncome,
    selectCounterpartType,
    selectCounterpart
}) {
    const year = selectYear;
    const prices = selectPrices;
    const continent = selectContinent;
    const income = selectIncome;
    const counterpartType = selectCounterpartType;
    const counterpart_area = selectCounterpart;

    const filteredData = data
        .filter((d) => year.includes(d.year))
        .filter((d) => d.prices === prices)
        .filter((d) => d.continent === continent || continent === "All")
        .filter((d) => income.includes(d.income_level))
        .filter((d) => d.counterpart_type === counterpartType || counterpartType === "All")
        .filter((d) => d.counterpart_area === counterpart_area || counterpart_area === "All");

    const options = Array.from(["All"]).concat(
        unique(filteredData.map((d) => d.country))
    );

    return Inputs.select(options, { label: "Country" });
}