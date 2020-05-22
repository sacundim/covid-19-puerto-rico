function testCompliance() {
    var local = Date.parse('2020-05-20T00:00:00');
    var utc = Date.parse('2020-05-20T00:00:00Z');
    return local != utc
}

function embedChart(name, bulletin_date) {
    if (testCompliance()) {
        vegaEmbed(`#${name}`, `${bulletin_date}_${name}.json`).catch(console.error);
    } else {
        var elem = document.createElement("img");
        elem.src = `${bulletin_date}_${name}.png`;
        document.getElementById(name).appendChild(elem);
    }
}

vega.timeFormatLocale(
    {
      "dateTime": "%x, %X",
      "date": "%d/%m/%Y",
      "time": "%-I:%M:%S %p",
      "periods": ["AM", "PM"],
      "days": ["domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"],
      "shortDays": ["dom", "lun", "mar", "mié", "jue", "vie", "sáb"],
      "months": ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"],
      "shortMonths": ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
    }
);