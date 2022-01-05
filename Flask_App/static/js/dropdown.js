/* When the user clicks on the button,
toggle between hiding and showing the dropdown content */
function showDropdown() {
  document.getElementById("tickerDropdown").classList.toggle("show");
}

function filterFunction() {
  var input, filter, ul, li, a, i;
  var counter = 0;
  var max_size = 8;
  input = document.getElementById("myInput");
  filter = input.value.toUpperCase();
  div = document.getElementById("tickerDropdown");
  a = div.getElementsByTagName("a");
  for (i = 0; i < a.length && counter<max_size; i++) {
    txtValue = a[i].textContent || a[i].innerText;
    if (txtValue.toUpperCase().indexOf(filter) > -1) {
      a[i].style.display = "";
      counter++;
    } else {
      a[i].style.display = "none";
    }
  }
}