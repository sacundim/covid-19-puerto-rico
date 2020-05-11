function embedChart(name, bulletin_date) {
  vegaEmbed(`#${name}`, `${bulletin_date}_${name}.json`).catch(console.error);
}
