#!/usr/bin/env node
// One-off: dump AniList's live schema to api/schema.graphql via introspection.
// The printed SDL is what graphql-js serves for __schema/__type queries on the
// mirror, so third-party clients (GraphiQL/Altair/Apollo) validate normally.
// Run: node scripts/dump_schema.mjs
import { writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildClientSchema, printSchema } from 'graphql';

const INTROSPECTION = `
query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types { ...FullType }
    directives { name description locations args { ...InputValue } }
  }
}
fragment FullType on __Type {
  kind name description
  fields(includeDeprecated: true) {
    name description
    args { ...InputValue }
    type { ...TypeRef }
    isDeprecated deprecationReason
  }
  inputFields { ...InputValue }
  interfaces { ...TypeRef }
  enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason }
  possibleTypes { ...TypeRef }
}
fragment InputValue on __InputValue {
  name description
  type { ...TypeRef }
  defaultValue
}
fragment TypeRef on __Type {
  kind name
  ofType { kind name
    ofType { kind name
      ofType { kind name
        ofType { kind name
          ofType { kind name
            ofType { kind name
              ofType { kind name } } } } } } }
}`;

const res = await fetch('https://graphql.anilist.co', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ query: INTROSPECTION }),
});
if (!res.ok) throw new Error(`HTTP ${res.status}`);
const body = await res.json();
if (body.errors) throw new Error(JSON.stringify(body.errors).slice(0, 500));

const sdl = printSchema(buildClientSchema(body.data));
const out = join(dirname(fileURLToPath(import.meta.url)), '..', 'api', 'schema.graphql');
writeFileSync(out, sdl);
console.log(`api/schema.graphql written: ${sdl.length} bytes, ${sdl.split('\n').length} lines`);
