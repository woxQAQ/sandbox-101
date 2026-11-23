// The package map aggregates distinct caller/call-site/target relationships. The
// function map keeps each symbol once, including when two branches share a callee.
const svgNS = 'http://www.w3.org/2000/svg';
const map = $('map');
const svgStyle = `
  .wire{fill:none;stroke:#648397;stroke-width:1.1;opacity:.35}
  .wire.dynamic{stroke-dasharray:5 3}.wire.callback{stroke-dasharray:2 4}.wire.handler{stroke:#397b78}
  .vertex{cursor:pointer;outline:none}.vertex text{font:14px 'IBM Plex Mono',monospace;fill:#203948}
  .vertex .sub{font:11px system-ui,sans-serif;fill:#526c7b}.vertex .frame{fill:white;stroke:#a9beca;stroke-width:1.3}
  .vertex.root .frame{fill:#e1edf5;stroke:#326787}.vertex.route .frame{fill:#ddede9;stroke:#398278}
  .vertex.boundary .frame{fill:#f2f5f6;stroke-dasharray:4 3}.vertex:hover text,.vertex:focus text{font-weight:bold}
  .vertex:focus .frame,.vertex:focus circle{stroke:#182d40;stroke-width:3}
  .dim{opacity:.18}.wire.dim{opacity:.055}.wire.from{stroke:#087772;opacity:.95;stroke-width:2.2}
  .wire.to{stroke:#a05229;opacity:.95;stroke-width:2.2}.vertex.selected .frame,.vertex.selected circle{stroke:#092d45;stroke-width:3}
  .vertex.selected text{font-weight:700}.heading{font:600 20px system-ui,sans-serif;fill:#213d50}
`;
function svg(tag, attributes = {}, text) {
  const e = document.createElementNS(svgNS,tag);
  for (const [key,value] of Object.entries(attributes)) e.setAttribute(key,value);
  if (text !== undefined) e.textContent = text;
  return e;
}
let packagesByName, owner, packageEdges, incoming, localGraph, selectedPackage, selectedFunction;
let shapes, wires, bounds, camera, drag;
function connect(edges, from, to, kind) {
  const key = JSON.stringify([from,to,kind]);
  if (!edges.has(key)) edges.set(key,{from,to,kind,count:0});
  const edge = edges.get(key); edge.count++; return edge;
}
function setupMap() {
  packagesByName = new Map(); owner = new Map(); incoming = new Map(); packageEdges = new Map();
  selectedPackage = null; selectedFunction = null; localGraph = null;
  for (const file of Object.values(data.files)) {
    if (typeof file?.package === 'string') packagesByName.set(file.package,0);
  }
  const names = [...packagesByName.keys()].sort((a,b) => b.length-a.length);
  // Synthetic functions can have no source position. Their qualified name still
  // identifies the declaring package (including bound method wrappers).
  for (const id of ids) {
    const filename = data.functions[id].at?.replace(/:\d+:\d+$/,'');
    const pkg = data.files[filename]?.package ?? names.find(p => id.startsWith(p+'.') || id.startsWith('(*'+p+'.') || id.startsWith('('+p+'.'));
    if (pkg) { owner.set(id,pkg); packagesByName.set(pkg,packagesByName.get(pkg)+1); }
  }
  for (const id of ids) {
    const from = owner.get(id);
    for (const call of list(data.functions[id].calls)) {
      for (const target of list(call.targets)) {
        if (!hasFunction(target)) continue;
        if (!incoming.has(target)) incoming.set(target,new Set());
        incoming.get(target).add(id);
        const to = owner.get(target);
        if (!from || !to || from === to) continue;
        const edge = connect(packageEdges,from,to,'package');
        if (call.dispatch === 'direct') edge.direct = true;
      }
    }
  }
}
function addNode(id, rank, kind) {
  if (!localGraph.nodes.has(id)) localGraph.nodes.set(id,{id,rank,kind});
}
function resetFunctionMap() {
  localGraph = {nodes:new Map(),edges:new Map()}; selectedFunction = active;
  const r = data.routes[routeIndex];
  if (r) {
    const root = `${r.surface} ${r.method} ${r.path ?? ''}`.trim(); addNode(root,0,'route');
    for (const h of r.handlers) { addNode(h.function,1,'local'); connect(localGraph.edges,root,h.function,'handler'); }
  }
  addNode(active,r ? 1 : 0,'local'); expandDown(active);
  if (!r) expandUp(active);
}
function targetKind(call) { return call.dispatch === 'direct' ? 'direct' : 'dynamic'; }
function addCall(from,to,kind,rank) {
  addNode(to,rank,hasFunction(to) ? 'local' : 'boundary');
  connect(localGraph.edges,from,to,kind);
}
function expandDown(id) {
  const node = localGraph.nodes.get(id);
  if (!hasFunction(id) || node.down) return;
  node.down = true;
  for (const call of list(data.functions[id].calls)) {
    for (const t of list(call.targets)) addCall(id,t,targetKind(call),node.rank+1);
    for (const t of list(call.function_args)) addCall(id,t,'callback',node.rank+1);
    if (!list(call.targets).length) {
      const label = call.dispatch === 'builtin' ? `builtin ${call.operand}` : `未解析 ${call.operand ?? call.dispatch} @ ${call.at ?? id}`;
      addCall(id,label,'boundary',node.rank+1);
    }
  }
}
function expandUp(id) {
  const node = localGraph.nodes.get(id);
  if (!hasFunction(id) || node.up) return;
  node.up = true;
  for (const parent of incoming.get(id) ?? []) {
    addNode(parent,node.rank-1,'local');
    // Downstream expansion may already have materialized these edges.
    if (localGraph.nodes.get(parent).down) continue;
    const kinds = new Set(list(data.functions[parent].calls).filter(c => list(c.targets).includes(id)).map(targetKind));
    for (const kind of kinds) connect(localGraph.edges,parent,id,kind);
  }
}
function packageLayout() {
  const labels = [...packagesByName.keys()].sort();
  const priority = ['github.com/woxQAQ/haven/cmd/haven','github.com/woxQAQ/haven/api','github.com/woxQAQ/haven/internal/application'];
  const order = [...priority.filter(p => packagesByName.has(p)),...labels.filter(p => !priority.includes(p))];
  const half = Math.ceil(order.length/2), height = Math.max(160,(half-1)*29);
  const nodes = order.map((id,i) => {
    const left = i < half, slot = left ? i : i-half;
    const fraction = slot / Math.max(1,(left ? half : order.length-half)-1);
    return {id,kind:'package',left,x:(left ? -1 : 1)*(148+109*Math.sin(fraction*Math.PI)),y:fraction*height};
  });
  return {nodes,edges:packageEdges.values(),extent:{x:-505,y:-45,w:1010,h:height+90}};
}
function functionLayout() {
  const nodes = [...localGraph.nodes.values()], ranks = new Map();
  for (const n of nodes) { if (!ranks.has(n.rank)) ranks.set(n.rank,[]); ranks.get(n.rank).push(n); }
  const maxRows = Math.max(...[...ranks.values()].map(rows=>rows.length));
  const levels = [...ranks.keys()], left = Math.min(...levels), right = Math.max(...levels);
  for (const [rank,column] of ranks) {
    column.sort((a,b) => Number(a.kind === 'boundary')-Number(b.kind === 'boundary') || a.id.localeCompare(b.id));
    column.forEach((n,i) => { n.x = (rank-left)*315; n.y = (i-(column.length-1)/2)*65; });
  }
  return {nodes,edges:localGraph.edges.values(),extent:{x:-166,y:-maxRows*32.5-22,w:(right-left)*315+332,h:maxRows*65+44}};
}
function shortPackage(pkg) { return pkg.replace(/^github\.com\/woxQAQ\/(haven\/)?/,''); }
function shortFunction(id) {
  const name = brief(id).split(' [')[0];
  const pkg = owner.get(id);
  return pkg ? name.replace(shortPackage(pkg)+'.','').replace(/^\(\*?([^()]+)\)\./,'$1.') : name;
}
function abbreviate(text,max) { return text.length>max ? text.slice(0,max-1)+'…' : text; }
function packagePath(a,b) {
  const inward = n => n.x+(n.left ? 1 : -1)*(a.left === b.left ? 130 : 172);
  const radius = n => 5+Math.min(5,Math.log1p(packagesByName.get(n.id))/1.45);
  const start = a.x+(a.left ? 1 : -1)*radius(a), end = b.x+(b.left ? 1 : -1)*radius(b);
  return `M${start},${a.y} C${inward(a)},${a.y} ${inward(b)},${b.y} ${end},${b.y}`;
}
function functionPath(a,b) {
  if (a.id === b.id) return `M${a.x+65},${a.y-22} c60,-28 -70,-28 -42,0`;
  if (a.x === b.x) return `M${a.x+123},${a.y} C${a.x+162},${a.y} ${b.x+162},${b.y} ${b.x+123},${b.y}`;
  const sign = Math.sign(b.x-a.x), start = a.x+123*sign, end = b.x-125*sign, mid = (start+end)/2;
  return `M${start},${a.y} C${mid},${a.y} ${mid},${b.y} ${end},${b.y}`;
}
const edgeNames = {direct:'直接调用',dynamic:'动态调用候选',handler:'注册 handler',callback:'传递函数参数',boundary:'内建或未解析',package:'跨包调用'};
function drawMap() {
  const overview = view === 'overview';
  if (!overview && !localGraph) resetFunctionMap();
  const layout = overview ? packageLayout() : functionLayout();
  bounds = layout.extent; shapes = new Map(); wires = [];
  const defs = svg('defs');
  for (const [name,color] of [['normal','#7893a2'],['from','#087772'],['to','#a05229']]) {
    const marker = svg('marker',{id:'arrow-'+name,viewBox:'0 0 8 8',markerWidth:6,markerHeight:6,refX:7,refY:4,orient:'auto',markerUnits:'userSpaceOnUse'});
    marker.append(svg('path',{d:'M0,0 L8,4 L0,8 Z',fill:color})); defs.append(marker);
  }
  map.replaceChildren(svg('style',{},svgStyle),defs);
  const byID = new Map(layout.nodes.map(n => [n.id,n]));
  const edgeLayer = svg('g'), nodeLayer = svg('g'); map.append(edgeLayer,nodeLayer);
  for (const edge of layout.edges) {
    const a=byID.get(edge.from), b=byID.get(edge.to);
    const kind = overview && !edge.direct ? 'dynamic' : edge.kind;
    const path = svg('path',{d:overview ? packagePath(a,b) : functionPath(a,b),class:'wire '+kind,'marker-end':'url(#arrow-normal)'});
    path.append(svg('title',{},`${brief(a.id)} → ${brief(b.id)} · ${edgeNames[kind]}${overview ? ' × '+edge.count : ''}`));
    edgeLayer.append(path); wires.push({edge,path});
  }
  for (const n of layout.nodes) {
    const g = svg('g',{class:'vertex '+n.kind+(n.id === active && !overview ? ' root' : ''),transform:`translate(${n.x} ${n.y})`,role:'button',tabindex:0,'aria-label':brief(n.id)});
    g.append(svg('title',{},brief(n.id)));
    if (overview) {
      const count = packagesByName.get(n.id);
      g.append(svg('circle',{r:3.6+Math.min(5,Math.log1p(count)/1.45),fill:n.left?'#56819d':'#53897e',stroke:'#fff','stroke-width':1.3}));
      g.append(svg('text',{x:n.left?-15:15,y:5,'text-anchor':n.left?'end':'start'},shortPackage(n.id)));
    } else {
      const pkg = owner.get(n.id);
      const label = n.kind === 'route' ? n.id : shortFunction(n.id);
      g.append(svg('rect',{class:'frame',x:-122,y:-22,width:244,height:44,rx:6}));
      g.append(svg('text',{x:-112,y:-4,class:'sub'},n.kind === 'route' ? 'HTTP 入口' : pkg ? shortPackage(pkg) : '外部 / 内建 / 未解析'));
      g.append(svg('text',{x:-112,y:13},abbreviate(label,27)));
    }
    const select = () => { if (overview) selectedPackage = n.id; else selectedFunction = n.id; highlight(n.id); updateSelection(); };
    g.onclick = select;
    g.onkeydown = e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); select(); } };
    g.onpointerenter = () => { if (!drag) highlight(n.id); };
    g.onpointerleave = () => highlight(overview ? selectedPackage : selectedFunction);
    nodeLayer.append(g); shapes.set(n.id,g);
  }
  const route = data.routes[routeIndex];
  $('mapTitle').textContent = overview ? `全程序 · ${layout.nodes.length} 个 Go 包` : route ? `${route.surface} · ${route.method} ${route.path ?? ''}` : brief(active);
  $('legend').textContent = overview ? '箭头由调用包指向被调用包。选中节点后，青色显示下游，棕色显示上游。虚线表示仅发现动态候选。' : '实线表示直接调用或路由注册，虚线表示动态候选，点线表示传递函数参数。选中节点可继续展开。';
  fitMap(); highlight(overview ? selectedPackage : selectedFunction); updateSelection();
}
function highlight(id) {
  const adjacent = new Set([id]);
  for (const {edge,path} of wires) {
    const from = edge.from === id, to = edge.to === id;
    if (from || to) { adjacent.add(edge.from); adjacent.add(edge.to); }
    path.classList.toggle('dim',Boolean(id) && !from && !to); path.classList.toggle('from',from); path.classList.toggle('to',to);
    path.setAttribute('marker-end',`url(#arrow-${from?'from':to?'to':'normal'})`);
  }
  for (const [key,g] of shapes) { g.classList.toggle('dim',Boolean(id) && !adjacent.has(key)); g.classList.toggle('selected',key === id); }
}
function updateSelection() {
  const overview = view === 'overview', id = overview ? selectedPackage : selectedFunction;
  const local = !overview && hasFunction(id);
  $('pkgFunctions').hidden = !overview || !id;
  for (const ctrl of ['up','down','inspect']) $(ctrl).hidden = !local;
  if (local) { const n = localGraph.nodes.get(id); $('up').disabled = n.up || !incoming.get(id)?.size; $('down').disabled = n.down || !list(data.functions[id].calls).length; }
  if (!id) { $('selection').textContent = `${packageEdges.size} 组跨包调用关系`; return; }
  const label = overview ? `${shortPackage(id)} · ${packagesByName.get(id)} 个函数` : brief(id);
  $('selection').textContent = label;
}
$('overview').onclick = () => { history.push({active,routeIndex,view}); view='overview'; routeIndex=-1; selectedPackage=null; renderIndex(); render(); };
$('pkgFunctions').onclick = () => { $('mode').value='functions'; $('search').value=selectedPackage+'.'; renderIndex(); };
$('up').onclick = () => { expandUp(selectedFunction); drawMap(); };
$('down').onclick = () => { expandDown(selectedFunction); drawMap(); };
$('inspect').onclick = () => { history.push({active,routeIndex,view}); active=selectedFunction; view='source'; renderIndex(); render(); };
function viewport() { map.setAttribute('viewBox',`${camera.x} ${camera.y} ${camera.w} ${camera.h}`); }
function fitMap() { camera = {...bounds}; viewport(); }
function zoom(factor, center) {
  if (!camera) return;
  const w = Math.max(bounds.w/24,Math.min(bounds.w*3,camera.w*factor));
  const scale = w/camera.w, p=center ?? {x:camera.x+camera.w/2,y:camera.y+camera.h/2};
  camera={x:p.x+(camera.x-p.x)*scale,y:p.y+(camera.y-p.y)*scale,w,h:camera.h*scale}; viewport();
}
function point(e) { return new DOMPoint(e.clientX,e.clientY).matrixTransform(map.getScreenCTM().inverse()); }
map.addEventListener('wheel',e => { e.preventDefault(); zoom(e.deltaY>0 ? 1.12 : 1/1.12,point(e)); },{passive:false});
map.onpointerdown = e => { if (!e.target.closest('.vertex') && e.button===0) { drag=point(e); map.setPointerCapture(e.pointerId); } };
map.onpointermove = e => { if (drag) { const p=point(e); camera.x+=drag.x-p.x; camera.y+=drag.y-p.y; viewport(); } };
map.onlostpointercapture = () => { drag=null; };
$('zoomIn').onclick = () => zoom(.8); $('zoomOut').onclick = () => zoom(1.25); $('fit').onclick = fitMap;
$('saveSVG').onclick = () => {
  const copy = map.cloneNode(true); copy.removeAttribute('id'); copy.setAttribute('xmlns',svgNS);
  copy.setAttribute('viewBox',`${bounds.x} ${bounds.y-38} ${bounds.w} ${bounds.h+38}`); copy.setAttribute('width',bounds.w); copy.setAttribute('height',bounds.h+38);
  copy.prepend(svg('rect',{x:bounds.x,y:bounds.y-38,width:bounds.w,height:bounds.h+38,fill:'#f8fbfc'}));
  copy.append(svg('text',{x:bounds.x+14,y:bounds.y-11,class:'heading'},$('mapTitle').textContent));
  const url = URL.createObjectURL(new Blob([new XMLSerializer().serializeToString(copy)],{type:'image/svg+xml'}));
  const a = element('a'); a.href=url; a.download=view==='overview'?'haven-packages.svg':'haven-calls.svg'; a.click(); setTimeout(()=>URL.revokeObjectURL(url),1000);
};
