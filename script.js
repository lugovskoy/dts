<script>
function myFunction(number)
{
    var text = document.getElementById("hidden" + number);
    var more = document.getElementById("more" + number);

    if (text.style.display == "none")
    {
        text.style.display = "block";
        more.innerHTML = "hide";
    }
    else
    {
        text.style.display = "none";
        more.innerHTML = "more";
    }
}
</script>
