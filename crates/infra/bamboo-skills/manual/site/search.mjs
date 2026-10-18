export function search(documents, query) {
  const terms = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  return documents.map(doc => {
    const fields = [doc.path.toLowerCase(), doc.title.toLowerCase(), doc.text.toLowerCase()];
    const score = terms.every(term => fields.some(field => field.includes(term)))
      ? terms.reduce((sum, term) => sum + fields.reduce((n, field, i) => n + (field.includes(term) ? [5,3,1][i] : 0), 0), 0) : -1;
    return {doc, score};
  }).filter(result => result.score >= 0).sort((a,b) => b.score-a.score || (a.doc.path < b.doc.path ? -1 : a.doc.path > b.doc.path ? 1 : 0)).map(result => result.doc);
}
