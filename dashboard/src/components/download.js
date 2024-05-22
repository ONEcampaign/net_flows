import * as XLSX from "npm:xlsx";

export function xlsxButton(data, filename = "data.xlsx") {
  if (!data) throw new Error("Array of data required as first argument");

  // Define the styles for the material design button
  const style = document.createElement("style");
  style.textContent = `
  .material-design-button {
    color: var(--text-color, black); /* Default text color black, overridden in dark mode */
    background-color: var(--theme-background, #ddd); /* Default background white, overridden in dark mode */
    font-size: 11px; /* Adjusted for better readability */
    padding: 6px 6px; /* More padding for a better touch target */
    border: 1.2px solid rgba(0, 0, 0, 0.1); /* Add a faint border */
    border-radius: 4px;
    cursor: pointer;
    text-decoration: none;
    display: inline-block;
    margin: 1px;

    transition: background-color 0.3s, box-shadow 0.2s;
  }

  .material-design-button:hover, .material-design-button:focus {
    box-shadow: 0 4px 5px rgba(0,0,0,0.30); /* Darker shadow for better contrast */
  }

  @media (prefers-color-scheme: dark) {
    :root {
      --text-color: white; /* White text in dark mode */
      --background-color: #333; /* Darker background in dark mode */
    }
    .material-design-button {
      box-shadow: 0 2px 3px rgba(255,255,255,0.1); /* Lighter shadow effect in dark mode */
    }
    .material-design-button:hover, .material-design-button:focus {
      box-shadow: 0 5px 6px rgba(255,255,255,0.15); /* More prominent shadow on hover/focus in dark mode */
    }
  }
`;
  document.head.appendChild(style);

  const button = document.createElement("button");
  button.className = "material-design-button";
  button.textContent = `Download ${filename}`;

  button.addEventListener("click", function () {
    // Convert data to worksheet
    const worksheet = XLSX.utils.json_to_sheet(data);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Sheet1");
    const wbout = XLSX.write(workbook, { bookType: "xlsx", type: "binary" });

    function s2ab(s) {
      const buffer = new ArrayBuffer(s.length);
      const view = new Uint8Array(buffer);
      for (let i = 0; i < s.length; i++) {
        view[i] = s.charCodeAt(i) & 0xff;
      }
      return buffer;
    }

    const downloadData = new Blob([s2ab(wbout)], {
      type: "application/octet-stream",
    });

    // Create download link
    const url = URL.createObjectURL(downloadData);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.style.display = "none"; // Hide the link
    document.body.appendChild(a);
    a.click(); // Simulate the click to trigger download

    // Clean up: revoke the object URL and remove the link after initiating the download
    URL.revokeObjectURL(url);
    a.remove();
  });

  return button;
}