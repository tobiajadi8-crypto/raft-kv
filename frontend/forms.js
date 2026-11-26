const form = document.querySelector('form');
const result = document.querySelector('#result');
function show(text, error = false) {
  result.hidden = false;
  result.dataset.error = String(error);
  result.textContent = text;
}
form.addEventListener('submit', async event => {
  event.preventDefault();
  const action = event.submitter?.value || 'read';
  const fields = Array.from(form.elements);
  fields.forEach(field => field.disabled = true);
  try {
    const key = document.querySelector('#key').value;
    const value = document.querySelector('#value');
    const read = action === 'read';
    show(read ? 'Reading…' : 'Saving…');
    const response = await fetch('/api/kv?key=' + encodeURIComponent(key), read ? {} : {method:'PUT', body:value.value});
    if (response.status === 404) return show('Key not found.', true);
    if (response.status === 503) return show('Leader unavailable. Try again after the election.', true);
    if (!response.ok) throw new Error(await response.text());
    if (read) {
      value.value = (await response.json()).value;
      show(value.value === '' ? 'Read an empty value.' : 'Loaded the saved value.');
    } else show('Value saved.');
  } catch (error) { show(error.message || 'Request failed.', true); }
  finally { fields.forEach(field => field.disabled = false); }
});
