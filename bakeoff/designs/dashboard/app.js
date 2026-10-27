(function(){
'use strict';
var TZ='America/Halifax',B=null,INS=null,cyc={},cdEl=null,startMs=null;
function $(i){return document.getElementById(i)}
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]})}
function has(v){return v!=null&&v!==''&&!(typeof v==='number'&&isNaN(v))}
function nz(v,d){return has(v)?esc(v):(d||'')}
var fT=new Intl.DateTimeFormat('en-CA',{timeZone:TZ,hour:'numeric',minute:'2-digit',hour12:true});
var fD=new Intl.DateTimeFormat('en-US',{timeZone:TZ,weekday:'short',month:'short',day:'numeric'});
function tm(iso){var d=new Date(iso);if(isNaN(d))return '';return fT.format(d).replace(/\s?[ap]\.?m\.?/i,function(m){return ' '+(/a/i.test(m)?'AM':'PM')}).replace(/\s+/g,' ')}
function dt(iso){var d=new Date(iso);return isNaN(d)?'':fD.format(d).replace(',','')}
function dd(s){if(!s)return '';var p=s.split('-');return dt(p[0]+'-'+p[1]+'-'+p[2]+'T12:00:00-03:00')}
function logo(u,cls){return has(u)?'<span class="disc '+(cls||'')+'"><img src="'+esc(u)+'" alt="" onerror="this.style.display=\'none\'"></span>':'<span class="disc '+(cls||'')+'"></span>'}
function head(u){return has(u)?'<img class="hs" src="'+esc(u)+'" alt="" onerror="this.style.visibility=\'hidden\'">':'<span class="hs"></span>'}
function ord(n){var s=['th','st','nd','rd'],v=n%100;return n+(s[(v-20)%10]||s[v]||s[0])}
function ft(url){return fetch(url,{cache:'no-store'}).then(function(r){return r.ok?r.json():null}).catch(function(){return null})}

/* generic cycling tile: views = array of html strings; rebuilt only when data refreshes */
function tile(key,bodyId,dotsId,views,ms,onShow){
  var body=$(bodyId),dots=$(dotsId),c=cyc[key];
  if(!c){c=cyc[key]={i:0,timer:null,els:[]};}
  if(c.timer){clearInterval(c.timer);c.timer=null}
  if(!views.length)views=['<div class="empty">Nothing to show right now</div>'];
  while(c.els.length<views.length){var v=document.createElement('div');v.className='view';body.appendChild(v);c.els.push(v)}
  while(c.els.length>views.length){body.removeChild(c.els.pop())}
  var dd2=dots.children;
  while(dd2.length<views.length)dots.appendChild(document.createElement('i'));
  while(dd2.length>views.length)dots.removeChild(dots.lastChild);
  for(var k=0;k<views.length;k++){c.els[k].innerHTML=views[k]}
  if(c.i>=views.length)c.i=0;
  function show(){for(var k=0;k<c.els.length;k++){c.els[k].classList.toggle('on',k===c.i);dots.children[k].classList.toggle('on',k===c.i)}if(onShow)onShow(c.i)}
  show();
  if(views.length>1)c.timer=setInterval(function(){c.i=(c.i+1)%views.length;show()},ms);
}

/* ---------- next game ---------- */
function cmpRow(l,a,b){if(!has(a)&&!has(b))return '';return '<div class="cmp"><span class="n">'+nz(a,'-')+'</span><span class="l">'+l+'</span><span class="n">'+nz(b,'-')+'</span></div>'}
function pct(v){return has(v)&&!isNaN(+v)?(+v).toFixed(1)+'%':null}
function pip(r){return '<span class="pip '+esc(r)+'">'+esc(r==='OTL'||r==='SOL'?'O':r)+'</span>'}
function renderNext(){
  var n=B.next_game,box=$('b-next');
  if(!n){startMs=null;box.innerHTML='<div class="empty">No upcoming game scheduled</div>';tileClear('next');return}
  
  var o=n.opponent||{},ts=B.team_stats||{},me=B.team||{};
  var opRec=(o.record||{}),gp=0;
  var oppGP=has(opRec.overall)?String(opRec.overall).split('-').reduce(function(a,x){return a+(+x||0)},0):0;
  startMs=new Date(n.start_iso).getTime();if(isNaN(startMs))startMs=null;
  var pre=
   '<div class="ng-top"><b>'+esc(dt(n.start_iso))+' &middot; '+esc(tm(n.start_iso))+'</b><span class="loc">'+(n.home?'HOME':'AWAY')+(has(n.venue)?' &middot; '+esc(n.venue):'')+'</span></div>'+
   '<div class="match"><div class="side">'+logo(me.logo_url)+'<div><div class="ab">'+esc(me.abbr||'AMH')+'</div><div class="rec">'+nz(ts.record)+'</div></div></div>'+
   '<div class="vs">'+(n.home?'VS':'AT')+'</div>'+
   '<div class="side r">'+logo(o.logo_url)+'<div><div class="ab">'+esc(o.abbr||'')+'</div><div class="rec">'+nz(opRec.overall)+'</div></div></div></div>'+
   '<div class="count" id="count"><div class="cd"><span class="num" id="c-d">0</span><small>days</small></div><div class="cd"><span class="num" id="c-h">00</span><small>hours</small></div><div class="cd"><span class="num" id="c-m">00</span><small>min</small></div><div class="cd"><span class="num" id="c-s">00</span><small>sec</small></div><div class="txt" id="c-t"></div></div>'+
   '<div class="sub" id="b-sub"></div>';
  var gf=has(o.gf)?o.gf+'-'+o.ga:null,mgf=has(ts.gf)?ts.gf+'-'+ts.ga:null;
  var v1=cmpRow('Record',ts.record,opRec.overall)+cmpRow('Last 10',ts.last10,opRec.last10)+cmpRow('Goals F-A',mgf,gf)+
    cmpRow('Power play',pct(ts.pp_pct),o.pp&&pct(o.pp.percentage))+cmpRow('Penalty kill',pct(ts.pk_pct),o.pk&&pct(o.pk.percentage));
  var v2=headToHead(n,o);
  var v3=oppLeaders(o);
  var views=[v1,v2,v3].filter(Boolean);
  var st=n.official_starters||{},h=st.home&&st.home.name,a=st.away&&st.away.name;
  var stTxt=(h||a)?'<b>'+esc(h||'TBA')+'</b> (home) &middot; <b>'+esc(a||'TBA')+'</b> (away)':'Starter TBA';
  box.innerHTML=pre+'<div class="start"><span>Official starters:</span> '+stTxt+'</div>';
  subCycle($('b-sub'),views,12000);
  cdEl={d:$('c-d'),h:$('c-h'),m:$('c-m'),s:$('c-s'),box:$('count'),t:$('c-t'),last:''};
  tickCountdown();
}
function tileClear(k){if(cyc[k]&&cyc[k].timer){clearInterval(cyc[k].timer);cyc[k].timer=null}}
var subState={i:0,timer:null,els:[]};
function subCycle(host,views,ms){
  if(subState.timer){clearInterval(subState.timer);subState.timer=null}
  subState.els=[];
  views.forEach(function(h){var v=document.createElement('div');v.className='view';v.innerHTML=h;host.appendChild(v);subState.els.push(v)});
  if(subState.i>=views.length)subState.i=0;
  function show(){subState.els.forEach(function(e,k){e.classList.toggle('on',k===subState.i)});var dots=$('d-next');while(dots.children.length<views.length)dots.appendChild(document.createElement('i'));while(dots.children.length>views.length)dots.removeChild(dots.lastChild);for(var k=0;k<dots.children.length;k++)dots.children[k].classList.toggle('on',k===subState.i)}
  show();
  if(views.length>1)subState.timer=setInterval(function(){subState.i=(subState.i+1)%views.length;show()},ms);
}
function headToHead(n,o){
  var g=(n.h2h_this_season||[]).concat(n.h2h_last_season||[]).slice(0,3);
  var rec=n.h2h_records&&n.h2h_records.last_5_years;
  if(!g.length&&!rec)return '';
  var h='<div class="cap">Head-to-head vs '+esc(o.abbr||'')+'</div>';
  if(rec&&rec.home_team&&rec.visiting_team){
    var a=n.home?rec.home_team:rec.visiting_team,b=n.home?rec.visiting_team:rec.home_team;
    if(has(a.w)&&has(b.w))h='<div class="cap">Head-to-head vs '+esc(o.abbr||'')+' &middot; last 5 years: '+esc(a.w)+'-'+esc(b.w)+(a.otl+a.sl+b.otl+b.sl?' (+extra time)':'')+'</div>'}
  if(!(n.h2h_this_season||[]).length)h+='<div class="row"><span class="grow m">No meetings yet this season</span></div>';
  g.forEach(function(x){if(!x.score)return;var r=x.result||'';
    h+='<div class="row"><span class="m" style="width:150px">'+esc(dd(x.date))+'</span><span class="grow">'+(x.home?'at home':'on the road')+'</span><span class="chip '+esc(r)+'">'+esc(r)+'</span><span class="v">'+esc(x.score['for'])+'-'+esc(x.score.against)+(x.ot_so?' '+esc(x.ot_so):'')+'</span></div>'});
  return h;
}
function oppLeaders(o){
  var s=(o.leading_scorers||[]).slice(0,2),g=(o.goalies||[]).slice(0,1),f=(B.next_game.opponent_form||{}).last5||[];
  if(!s.length&&!g.length&&!f.length)return '';
  var h='<div class="cap">'+esc(o.abbr||'')+' scorers &amp; form</div>';
  s.forEach(function(p){h+='<div class="row">'+head(p.headshot_url)+'<span class="grow">'+esc(p.name)+'</span><span class="m">'+esc(p.g)+'G '+esc(p.a)+'A</span><span class="v">'+esc(p.pts)+'</span></div>'});
  g.forEach(function(p){if(has(p.sv_pct))h+='<div class="row"><span class="m" style="width:44px">G</span><span class="grow">'+esc(p.name)+' <span class="m">season</span></span><span class="m">'+esc((+p.sv_pct).toFixed(3).replace(/^0/,''))+' SV% &middot; '+esc(p.gaa)+' GAA</span></div>'});
  if(f.length){h+='<div class="row"><span class="grow m">'+esc(o.abbr||'')+' last '+Math.min(5,f.length)+' games</span><span>'+f.slice(0,5).map(function(x){return pip(x.result)}).join(' ')+'</span></div>'}
  return h;
}
function tickCountdown(){
  if(!cdEl||startMs==null)return;
  var ms=startMs-Date.now(),c=cdEl;
  if(ms<=0){var txt=ms>-3*3600e3?'PUCK DROP':'GAME DAY';if(c.last!==txt){c.last=txt;c.box.classList.add('live');c.t.textContent=txt}return}
  c.box.classList.remove('live');
  var s=Math.floor(ms/1000),d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60),x=s%60;
  var v=[String(d),pad(h),pad(m),pad(x)];
  if(c.d.textContent!==v[0])c.d.textContent=v[0];if(c.h.textContent!==v[1])c.h.textContent=v[1];if(c.m.textContent!==v[2])c.m.textContent=v[2];c.s.textContent=v[3];
}
function pad(n){return n<10?'0'+n:''+n}

/* ---------- standings ---------- */
function stView(div){
  var rows=(div.rows||[]).slice(0,6),h='<table class="tbl"><tr><th>#</th><th></th><th>TEAM</th><th>GP</th><th>W</th><th>L</th><th>OT</th><th>PTS</th></tr>';
  rows.forEach(function(r){h+='<tr class="'+(r.is_ramblers?'me':'')+'"><td class="rk">'+esc(r.rank)+'</td><td class="lg">'+logo(r.logo_url)+'</td><td class="t">'+esc(r.team)+'</td><td>'+nz(r.gp)+'</td><td>'+nz(r.w)+'</td><td>'+nz(r.l)+'</td><td>'+esc((+r.otl||0)+(+r.sol||0))+'</td><td class="pts">'+nz(r.pts)+'</td></tr>'});
  return h+'</table>';
}
function renderStand(){
  var ds=(B.standings&&B.standings.divisions)||[];
  tile('stand','b-stand','d-stand',ds.map(stView),14000,function(i){var d=ds[i];$('h-stand').textContent=d?d.name+' standings':'Standings'});
}

/* ---------- scorers ---------- */
function renderScorers(){
  var sk=(B.skaters||[]).slice(),defs=[['pts','Points','points'],['g','Goals','goals'],['a','Assists','assists']];
  var views=defs.map(function(d){
    var l=sk.filter(function(p){return has(p[d[0]])}).sort(function(a,b){return b[d[0]]-a[d[0]]||b.pts-a.pts}).slice(0,5);
    return l.map(function(p,i){return '<div class="sr"><span class="rk">'+(i+1)+'</span>'+head(p.headshot_url)+'<span class="nm">'+esc(p.name)+(has(p.number)?'<span class="no">#'+esc(p.number)+'</span>':'')+'</span><span class="x">'+esc(p.g)+'G '+esc(p.a)+'A &middot; '+esc(p.gp)+' GP</span><span class="v">'+esc(p[d[0]])+'</span></div>'}).join('');
  }).filter(function(x){return x});
  tile('sc','b-sc','d-sc',views,9000,function(i){$('h-sc').textContent='Ramblers leaders: '+defs[i][1]});
}

/* ---------- results ---------- */
function renderRes(){
  var r=(B.recent||[]).slice(0,8),views=[];
  for(var i=0;i<r.length;i+=4){views.push(r.slice(i,i+4).map(function(g){
    var sc=g.score?esc(g.score['for'])+'-'+esc(g.score.against):'';
    return '<div class="rr"><span class="dt">'+esc(dd(g.date))+'</span>'+logo(g.opp&&g.opp.logo_url)+'<span class="op">'+(g.home?'vs ':'at ')+esc(g.opp&&(g.opp.abbr||g.opp.name)||'')+(g.ot_so?' <span style="color:var(--mut)">('+esc(g.ot_so)+')</span>':'')+'</span><span class="chip '+esc(g.result)+'">'+esc(g.result||'')+'</span><span class="sc">'+sc+'</span></div>'}).join(''))}
  tile('res','b-res','d-res',views,13000);
}

/* ---------- insights ---------- */
function insightItems(){
  var out=[];
  if(INS&&!INS.stub&&INS.items){INS.items.slice().sort(function(a,b){return (a.priority||9)-(b.priority||9)}).forEach(function(it){if(it&&has(it.headline))out.push({kind:it.kind,h:it.headline,d:it.detail,v:it.stat&&it.stat.value,l:it.stat&&it.stat.label})})}
  if(out.length>=2)return out.slice(0,6);
  var ts=B.team_stats||{},lr=ts.league_ranks||{},n=lr.of_teams;
  var rr=B.league_leaders&&B.league_leaders.ramblers_ranks,p=rr&&rr.points&&rr.points[0];
  if(p)out.push({kind:'Scoring',h:p.name+' is '+ord(p.rank)+' in the MHL in points',d:p.value+' points in '+((B.skaters||[])[0]||{}).gp+' games for the Ramblers.',v:ord(p.rank),l:'MHL points'});
  if(has(lr.pk_pct)&&has(n)&&has(ts.pk_pct))out.push({kind:'Special teams',h:'Penalty kill is '+ord(lr.pk_pct)+' of '+n+' teams',d:'Killing '+(+ts.pk_pct).toFixed(1)+'% of opponent power plays.',v:(+ts.pk_pct).toFixed(1)+'%',l:'penalty kill'});
  (B.milestones_near||[]).slice(0,2).forEach(function(m){out.push({kind:'Milestone',h:m.name+' needs '+m.needs+' to reach '+m.milestone,d:m.stat+': '+m.current+' so far.',v:m.needs,l:'to '+m.milestone})});
  if(has(ts.shots_for_pg)&&has(ts.shots_against_pg))out.push({kind:'Shots',h:'Ramblers average '+ts.shots_for_pg+' shots per game',d:'Opponents average '+ts.shots_against_pg+' against.',v:ts.shots_for_pg,l:'shots / game'});
  return out.slice(0,6);
}
function renderIns(){
  var views=insightItems().map(function(it){
    return '<div class="ins">'+(has(it.v)?'<div class="st"><div class="num'+(String(it.v).length>4?' long':'')+'">'+esc(it.v)+'</div>'+(has(it.l)?'<small>'+esc(it.l)+'</small>':'')+'</div>':'')+'<div class="tx">'+(has(it.kind)?'<div class="kind">'+esc(it.kind)+'</div>':'')+'<h3>'+esc(it.h)+'</h3>'+(has(it.d)?'<p>'+esc(it.d)+'</p>':'')+'</div></div>'});
  tile('ins','b-ins','d-ins',views.map(function(v){return '<div style="height:100%;display:flex;align-items:center">'+v+'</div>'}),10000);
}

function renderAll(){
  if(!B)return;
  var t=B.team||{};
  if(has(t.logo_url))$('hlogo').src=t.logo_url;
  $('hsub').textContent='Game TV'+(has(t.division)?' · '+t.division:'');
  $('foot').textContent=has(B.copyright)?B.copyright:'Official statistics provided by Maritime Hockey League. Powered by HockeyTech.com';
  renderNext();renderStand();renderScorers();renderRes();renderIns();
}
function load(){
  Promise.all([ft('../../data/board.json'),ft('../../data/insights.json')]).then(function(r){
    if(r[0])B=r[0];INS=r[1];renderAll();
  });
}
var fC=new Intl.DateTimeFormat('en-US',{timeZone:TZ,hour:'numeric',minute:'2-digit',hour12:true});
function clock(){$('clock').textContent=fC.format(new Date()).replace(/\s/g,' ')}
clock();setInterval(function(){clock();tickCountdown()},1000);
load();setInterval(load,300000);
})();
