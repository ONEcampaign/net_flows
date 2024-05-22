// See https://observablehq.com/framework/config for documentation.
export default {
  // The project’s title; used in the sidebar and webpage titles.
  title: "Data Explorer",

  // The pages and sections in the sidebar. If you don’t specify this option,
  // all pages will be listed in alphabetical order. Listing pages explicitly
  // lets you organize them into sections and have unlisted pages.
  pages: [
        {name: "Data Dive", path: "https://data.one.org/data-dives/net-finance-flows-to-developing-countries/"},
        {name: "Methodology", path: "https://observablehq.com/@one-campaign/net-flows"}
      ],

  // Some additional configuration options and their defaults:
  theme: "dashboard", // try "light", "dark", "slate", etc.
  // header: "", // what to show in the header (HTML)
  footer: "The ONE Campaign", // what to show in the footer (HTML)
  toc: false, // whether to show the table of contents
  pager: false, // whether to show previous & next links in the footer
  root: "src", // path to the source root for preview
  // output: "dist", // path to the output root for build
  // search: true, // activate search
};
