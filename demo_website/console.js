function runQuery() {
    // Get the SQL query entered by the user
    const query = document.getElementById("queryInput").value;
  
    // Send an AJAX request to the server-side script with the query as a parameter
    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/run_query");
    xhr.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
    xhr.onload = function () {
      // Display the query results in the text area
      document.getElementById("queryResult").value = xhr.responseText;
    };
    xhr.send("query=" + encodeURIComponent(query));
  }
  