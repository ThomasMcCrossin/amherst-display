(function(){
'use strict';
var DWELL=12000, TZ='America/Halifax';
var B=null, INS=null, panels=[], cur=-1, timer=0, cdT=0;
var $=function(id){return document.getElementById(id);};
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});}
function has(v){return v!==null&&v!==undefined&&v!==''&&!(typeof v==='number'&&isNaN(v));}
function num(v,d){return has(v)&&isFinite(+v)?(+v).toFixed(d==null?0:d):'';}
function ord(n){n=+n;if(!n)return '';var s=['th','st','nd','rd'],v=n%100;return n+(s[(v-20)%10]||s[v]||s[0]);}
function img(u,cls){return has(u)?'<img'+(cls?' class="'+cls+'"':'')+' src="'+esc(u)+'" alt="" onerror="this.style.visibility=\'hidden\'">':'';}
function disc(u){return '<div class="disc">'+img(u)+'</div>';}
function fmtDate(iso){var d=new Date(iso);if(isNaN(d))return '';var p={};new Intl.DateTimeFormat('en-US',{timeZone:TZ,weekday:'short',month:'short',day:'numeric'}).formatToParts(d).forEach(function(x){p[x.type]=x.value;});return p.weekday+' '+p.month+' '+p.day;}
function fmtTime(iso){var d=new Date(iso);if(isNaN(d))return '';return new Intl.DateTimeFormat('en-US',{timeZone:TZ,hour:'numeric',minute:'2-digit',hour12:true}).format(d).replace(/[  ]/g,' ');}
function fmtYmd(s){if(!s)return '';var d=new Date(s+'T12:00:00Z');if(isNaN(d))return '';var p={};new Intl.DateTimeFormat('en-US',{timeZone:'UTC',weekday:'short',month:'short',day:'numeric'}).formatToParts(d).forEach(function(x){p[x.type]=x.value;});return p.weekday+' '+p.month+' '+p.day;}
function streak(s){var m=String(s||'').split('-').map(Number);if(m.length!==4||m.some(isNaN))return '';var l=m[1]+m[2]+m[3];if(m[0]>0&&!l)return 'W'+m[0];if(!m[0]&&l)return 'L'+l;return '';}
function rcls(r){return r==='W'?'W':(r==='L'?'L':'O');}
function rlab(r){return r==='W'?'W':(r==='L'?'L':'OT');}
function fetchJson(u){return fetch(u+'?t='+Date.now(),{cache:'no-store'}).then(function(r){if(!r.ok)throw new Error(r.status);return r.json();}).catch(function(){return null;});}

/* ---------- derived content ---------- */
function ourTeam(){return (B.team&&B.team.abbr)||'AMH';}
function realInsights(){
  if(!INS||INS.stub||!INS.items)return [];
  return INS.items.filter(function(i){return i&&i.headline&&!/^stub/.test(i.id||'');}).sort(function(a,b){return (a.priority||9)-(b.priority||9);});
}
function factItems(){
  var out=[],ts=B.team_stats||{},lr=ts.league_ranks||{},n=lr.of_teams||'';
  var sk=(B.skaters||[])[0];
  if(sk&&has(sk.pts)){var rk=(((B.league_leaders||{}).ramblers_ranks||{}).points||[])[0];
    out.push({kind:'TEAM LEADER',headline:sk.name+' leads the Ramblers in scoring',detail:sk.g+' goals, '+sk.a+' assists in '+sk.gp+' games'+(rk&&rk.rank?' - '+ord(rk.rank)+' in the MHL':''),stat:{value:String(sk.pts),label:'POINTS'}});}
  if(has(ts.pk_pct)&&lr.pk_pct)out.push({kind:'SPECIAL TEAMS',headline:'Penalty kill ranks '+ord(lr.pk_pct)+' of '+n,detail:'Killing '+num(ts.pk_pct,1)+'% of penalties; power play is '+num(ts.pp_pct,1)+'% ('+ts.pp+')',stat:{value:num(ts.pk_pct,1)+'%',label:'PENALTY KILL'}});
  var m=(B.milestones_near||[])[0];
  if(m)out.push({kind:'MILESTONE WATCH',headline:m.name+' is '+m.needs+' away from '+m.milestone,detail:m.stat.replace(/^career /,'Career ')+': '+m.current+' so far, '+m.milestone+' is next',stat:{value:String(m.needs),label:'TO GO'}});
  if(has(ts.shots_for_pg))out.push({kind:'SHOTS',headline:'Ramblers average '+num(ts.shots_for_pg,1)+' shots a game',detail:'Opponents average '+num(ts.shots_against_pg,1)+' (last '+ts.shots_sample_games+' games)',stat:{value:num(ts.shots_for_pg,1),label:'SHOTS / GAME'}});
  return out;
}
function insightCards(){
  var r=realInsights().map(function(i){return {kind:String(i.kind||'STORYLINE').toUpperCase(),headline:i.headline,detail:i.detail,stat:i.stat&&has(i.stat.value)?i.stat:null};});
  return r.concat(factItems()).slice(0,3);
}

/* ---------- panel builders ---------- */
function pillsFromRecent(){return (B.recent||[]).slice(0,5).reverse().map(function(g){return g.result;});}
function pillsHtml(arr){return arr.map(function(r){return '<div class="pill '+rcls(r)+'">'+rlab(r)+'</div>';}).join('');}
function oppPills(ng){var f=((ng.opponent_form||{}).last5||[]).slice().reverse().map(function(g){return g.result;});return f;}
function tapeHtml(){
  var ng=B.next_game;if(!ng)return '<div class="hd"><div class="slab">TALE OF THE TAPE</div></div><div class="sub rise" style="padding:30px 0">Next game to be announced</div>';
  var o=ng.opponent||{},os=o.standings||{},ts=B.team_stats||{},pp=o.pp||{},pk=o.pk||{},gp=+os.gp||0;
  var us={name:'AMHERST RAMBLERS',logo:B.team.logo_url,rank:ts.division_rank?ord(ts.division_rank)+' '+B.team.division:'',rec:ts.record,pts:ts.pts,gf:ts.gf_pg,ga:ts.ga_pg,pp:ts.pp_pct,pk:ts.pk_pct,form:pillsFromRecent(),cls:'us'};
  var th={name:(o.name||'').toUpperCase(),logo:o.logo_url,rank:os.rank?ord(os.rank)+' '+(o.division||''):'',rec:(o.record||{}).overall,pts:os.pts,gf:gp&&has(o.gf)?o.gf/gp:null,ga:gp&&has(o.ga)?o.ga/gp:null,pp:pp.percentage,pk:pk.percentage,form:oppPills(ng),cls:'them'};
  var L=ng.home?th:us, R=ng.home?us:th;
  function side(t,tag,i){return '<div class="side '+t.cls+' rise" style="--i:'+i+'">'+disc(t.logo)+'<div class="tag">'+tag+'</div><div class="tn">'+esc(t.name)+'</div><div class="tr">'+esc(t.rank)+'</div>'+(t.form.length?'<div class="form"><span class="fl">LAST '+t.form.length+'</span>'+pillsHtml(t.form)+'</div>':'')+'</div>';}
  var defs=[['RECORD','rec',0,0],['POINTS','pts',0,1],['GOALS FOR / GM','gf',2,1],['GOALS AGAINST / GM','ga',2,-1],['POWER PLAY','pp',1,1],['PENALTY KILL','pk',1,1]],rows='';
  defs.forEach(function(d,k){
    function f(t){var v=t[d[1]];if(!has(v))return '--';return d[1]==='rec'?v:(d[1]==='pts'?String(v):num(v,d[2]===2?2:d[2])+(d[1]==='pp'||d[1]==='pk'?'%':''));}
    var a=L[d[1]],b=R[d[1]],la='',lb='';
    if(d[3]!==0&&has(a)&&has(b)&&+a!==+b){var aw=(+a>+b)===(d[3]>0);la=aw?' lead':'';lb=aw?'':' lead';}
    rows+='<div class="row rise" style="--i:'+(k+1)+'"><div class="v'+la+'">'+esc(f(L))+'</div><div class="lab">'+d[0]+'</div><div class="v'+lb+'">'+esc(f(R))+'</div></div>';});
  var st=ng.official_starters||{},sh=st.home&&st.home.name,sa=st.away&&st.away.name,gtxt;
  if(sh||sa){var parts=[];if(sa)parts.push((ng.home?o.abbr:ourTeam())+' '+sa);if(sh)parts.push((ng.home?ourTeam():o.abbr)+' '+sh);gtxt=parts.join('  /  ');}else gtxt='Starter TBA';
  var h=(ng.h2h_this_season&&ng.h2h_this_season.length)?ng.h2h_this_season:ng.h2h_last_season||[],hl=(ng.h2h_this_season&&ng.h2h_this_season.length)?'HEAD TO HEAD THIS SEASON':'HEAD TO HEAD LAST SEASON';
  var ht=h.length?h.slice(0,3).map(function(g){return (g.result==='W'?'W':g.result==='L'?'L':g.result)+' '+g.score.for+'-'+g.score.against+(g.home?' home':' away');}).join('   |   '):'First meeting of the season';
  return '<div class="hd rise"><div class="slab">TALE OF THE TAPE</div><div class="sub">'+esc(fmtDate(ng.start_iso))+' &middot; '+esc(fmtTime(ng.start_iso))+' &middot; '+esc((ng.venue||'').toUpperCase())+'</div></div>'+
   '<div class="tape">'+side(L,'AWAY',0).replace('AWAY',ng.home?'AWAY':'AWAY')+'<div class="mid">'+rows+'</div>'+side(R,'HOME',0)+'</div>'+
   '<div class="tfoot rise" style="--i:8"><div class="tf"><div class="k">STARTING GOALIE</div><div class="t">'+esc(gtxt)+'</div></div><div class="tf"><div class="k">'+hl+'</div><div class="t">'+esc(ht)+'</div></div></div>';
}
function standingsHtml(){
  var divs=(B.standings||{}).divisions||[],d=null;
  divs.forEach(function(x){if(x.rows.some(function(r){return r.is_ramblers;}))d=x;});if(!d)d=divs[0];if(!d)return '';
  var rows=d.rows.slice();
  if(rows.length>7){var ix=rows.findIndex(function(r){return r.is_ramblers;});var s=Math.max(0,Math.min(ix-3,rows.length-7));rows=rows.slice(s,s+7);}
  var h='<div class="hd rise"><div class="slab">STANDINGS</div><div class="sub">'+esc((d.name||'').toUpperCase())+' DIVISION</div></div><div class="tbl">'+
   '<div class="tr0 rise"><span>RK</span><span></span><span>TEAM</span><span>GP</span><span>W</span><span>L</span><span>OTL</span><span>STREAK</span><span>PTS</span></div>';
  rows.forEach(function(r,i){var ot=(+r.otl||0)+(+r.sol||0);
    h+='<div class="tr1 rise'+(r.is_ramblers?' us':'')+'" style="--i:'+(i+1)+'"><span class="rk">'+esc(r.rank)+'</span>'+disc(r.logo_url)+'<span class="nm">'+esc(r.team)+'</span><span>'+esc(r.gp)+'</span><span>'+esc(r.w)+'</span><span>'+esc(r.l)+'</span><span>'+ot+'</span><span class="st">'+streak(r.streak)+'</span><span class="pt">'+esc(r.pts)+'</span></div>';});
  return h+'</div>';
}
function leadersHtml(){
  var sk=(B.skaters||[]).slice(0,4),rk=(((B.league_leaders||{}).ramblers_ranks||{}).points)||[],h='<div class="hd rise"><div class="slab">RAMBLERS LEADERS</div><div class="sub">TEAM SCORING &middot; '+esc(B.season||'')+'</div></div><div class="lead4">';
  sk.forEach(function(p,i){var r=rk.filter(function(x){return x.id===p.id;})[0];
    h+='<div class="lc rise" style="--i:'+i+'"><div class="ph">'+img(p.headshot_url)+(has(p.number)?'<div class="no">#'+esc(p.number)+'</div>':'')+(r&&r.rank?'<div class="rkb">'+ord(r.rank)+' IN MHL</div>':'')+'</div><div class="nm">'+esc(p.name)+'</div><div class="ps">'+esc(p.pos||'')+(p.gp?' &middot; '+p.gp+' GP':'')+'</div><div class="big">'+esc(p.pts)+'<small>PTS</small></div><div class="ga">'+esc(p.g)+' G &nbsp;&nbsp; '+esc(p.a)+' A</div></div>';});
  h+='</div><div class="gstrip">';
  (B.goalies||[]).filter(function(g){return g.gp;}).slice(0,2).forEach(function(g,i){
    h+='<div class="gs rise" style="--i:'+(i+4)+'">'+img(g.headshot_url)+'<div><div class="k">GOALIE</div><div class="n">'+esc(g.name)+'</div></div><div class="nums"><div>'+(has(g.sv_pct)?num(g.sv_pct,3).replace(/^0/,''):'--')+'<small>SV%</small></div><div>'+(has(g.gaa)?num(g.gaa,2):'--')+'<small>GAA</small></div><div>'+esc(g.w||0)+'-'+esc(g.l||0)+'<small>W-L</small></div></div></div>';});
  return h+'</div>';
}
function lastGameHtml(){
  var g=(B.recent||[])[0];if(!g)return '';
  var win=g.result==='W',ot=g.ot_so?(' / '+g.ot_so):'',us=ourTeam();
  function tm(abbr,logo,sc,w){return '<div class="tm">'+disc(logo)+'<div class="a">'+esc(abbr)+'</div><div class="s '+(w?'w':'l')+'">'+esc(sc)+'</div></div>';}
  var left=g.home?tm((g.opp||{}).abbr,(g.opp||{}).logo_url,g.score.against,!win):tm(us,B.team.logo_url,g.score.for,win);
  var right=g.home?tm(us,B.team.logo_url,g.score.for,win):tm((g.opp||{}).abbr,(g.opp||{}).logo_url,g.score.against,!win);
  if(g.home){left=tm(us,B.team.logo_url,g.score.for,win);right=tm((g.opp||{}).abbr,(g.opp||{}).logo_url,g.score.against,!win);}
  var chips=(B.recent||[]).slice(0,5).map(function(r){return '<div class="chip '+rcls(r.result)+'"><b>'+r.score.for+'-'+r.score.against+'</b><span>'+(r.home?'vs ':'@ ')+esc((r.opp||{}).abbr)+'</span></div>';}).join('');
  var stars=(g.three_stars||[]).slice(0,3).map(function(s,i){return '<div class="star rise" style="--i:'+(i+1)+'"><div class="ph">'+img('https://assets.leaguestat.com/mhl/240x240/'+s.id+'.jpg')+'</div><div class="sn">&#9733; '+s.star+'</div><div class="nm">'+esc(s.name)+'</div><div class="ln">'+esc(s.team)+' &middot; '+esc(s.line||'')+'</div></div>';}).join('');
  var gl=(g.goals||[]).slice(0,5).map(function(x){return '<div class="g'+(x.mine?' mine':'')+'"><span class="tm0">'+esc(x.team)+'</span><span class="pd">'+esc(x.period)+' '+esc(x.time)+'</span><span class="who">'+esc(x.scorer)+(x.assists&&x.assists.length?'<small>'+esc(x.assists.join(', '))+'</small>':'')+(x.pp?'<small>PP</small>':'')+'</span></div>';}).join('');
  return '<div class="hd rise"><div class="slab">LAST GAME</div><div class="sub">'+esc(fmtYmd(g.date))+' &middot; '+(g.home?'AT HOME':'ON THE ROAD')+'</div></div><div class="lg"><div class="sc rise"><div class="fin">FINAL'+esc(ot)+'</div><div class="fin2">'+esc((g.venue||'').toUpperCase())+(g.attendance?' &middot; '+g.attendance+' FANS':'')+'</div><div class="teams">'+left+right+'</div><div class="res5"><div class="rh">LAST 5 RESULTS</div><div class="chips">'+chips+'</div></div></div>'+
   '<div class="rt">'+(stars?'<div class="stars">'+stars+'</div>':'')+(gl?'<div class="goals rise" style="--i:4"><div class="gh">SCORING</div>'+gl+'</div>':'')+'</div></div>';
}
function insightsHtml(){
  var c=insightCards();
  return '<div class="hd rise"><div class="slab">STORYLINES</div><div class="sub">AROUND THE RAMBLERS</div></div><div class="ins">'+c.map(function(x,i){
   return '<div class="ic rise" style="--i:'+i+'"><div class="kd">'+esc(x.kind)+'</div>'+(x.stat?'<div class="sv">'+esc(x.stat.value)+'</div><div class="sl">'+esc((x.stat.label||'').toUpperCase())+'</div>':'')+'<div class="hl">'+esc(x.headline)+'</div>'+(x.detail?'<div class="dt">'+esc(x.detail)+'</div>':'')+'</div>';}).join('')+'</div>';
}
function scheduleHtml(){
  var up=(B.upcoming||[]).slice(0,5),ts=B.team_stats||{},lr=ts.league_ranks||{},n=lr.of_teams?' OF '+lr.of_teams:'';
  var h='<div class="hd rise"><div class="slab">COMING UP</div><div class="sub">SCHEDULE &amp; BY THE NUMBERS</div></div><div class="sched"><div class="sl">';
  up.forEach(function(g,i){var o=g.opp||{};
    h+='<div class="sr rise'+(i===0?' first':'')+'" style="--i:'+i+'"><div class="dd">'+esc(fmtDate(g.start_iso).toUpperCase())+'<small>'+esc(fmtTime(g.start_iso))+'</small></div>'+disc(o.logo_url)+'<div class="on">'+esc((o.name||'').toUpperCase())+'<small>'+esc((g.venue||'').toUpperCase())+'</small></div><div class="ha'+(g.home?'':' away')+'">'+(g.home?'HOME':'AWAY')+'</div></div>';});
  h+='</div><div class="nums6">';
  var t=[[ts.gf_pg,2,'GOALS FOR / GM',lr.gf],[ts.ga_pg,2,'GOALS AGAINST / GM',lr.ga],[ts.pp_pct,1,'POWER PLAY %',lr.pp_pct],[ts.pk_pct,1,'PENALTY KILL %',lr.pk_pct]];
  t.forEach(function(x,i){if(!has(x[0]))return;h+='<div class="nt rise" style="--i:'+(i+1)+'"><div class="nv">'+num(x[0],x[1])+'</div><div class="nl">'+x[2]+'</div>'+(x[3]?'<div class="nr">'+ord(x[3]).toUpperCase()+n+'</div>':'')+'</div>';});
  return h+'</div></div>';
}
var BUILD=[['TALE OF THE TAPE',tapeHtml],['STANDINGS',standingsHtml],['RAMBLERS LEADERS',leadersHtml],['LAST GAME',lastGameHtml],['STORYLINES',insightsHtml],['COMING UP',scheduleHtml]];

/* ---------- ticker ---------- */
function tickerItems(){
  var it=[],us=ourTeam();
  (B.recent||[]).slice(0,4).forEach(function(g){var o=(g.opp||{}).abbr||'';var a=g.home?us+' '+g.score.for+'  '+o+' '+g.score.against:o+' '+g.score.against+'  '+us+' '+g.score.for;it.push(['FINAL'+(g.ot_so?' '+g.ot_so:''),a+'  ('+fmtYmd(g.date)+')']);});
  var ng=B.next_game;if(ng)it.push(['NEXT',(ng.home?'vs ':'at ')+((ng.opponent||{}).name||'')+' - '+fmtDate(ng.start_iso)+', '+fmtTime(ng.start_iso)]);
  realInsights().forEach(function(i){it.push([String(i.kind||'INSIGHT').toUpperCase(),i.headline]);});
  var d=null;((B.standings||{}).divisions||[]).forEach(function(x){if(x.rows.some(function(r){return r.is_ramblers;}))d=x;});
  if(d&&d.rows[0])it.push(['STANDINGS',d.rows[0].team+' lead '+d.name+' with '+d.rows[0].pts+' points']);
  factItems().forEach(function(f){it.push([f.kind,f.headline]);});
  var m=B.milestones_near||[];if(m[1])it.push(['MILESTONE',m[1].name+' needs '+m[1].needs+' for '+m[1].milestone+' '+m[1].stat.replace('career MHL ','career ')]);
  return it;
}
function buildTicker(){
  var run=$('tickRun'),it=tickerItems();
  var one=it.map(function(x){return '<span><b>'+esc(x[0])+'</b>'+esc(x[1])+'</span><em>&#9670;</em>';}).join('');
  run.innerHTML=one+one;
  var w=run.scrollWidth/2||3000;
  run.style.setProperty('--tdur',Math.max(40,Math.round(w/110))+'s');
  run.style.animation='none';void run.offsetWidth;run.style.animation='';
}

/* ---------- bug ---------- */
function buildBug(){
  var ts=B.team_stats||{};
  $('bugLogo').src=B.team.logo_url;
  $('bugRec').textContent=(ts.record||'')+(has(ts.pts)?'  ·  '+ts.pts+' PTS':'')+(ts.division_rank?'  ·  '+ord(ts.division_rank).toUpperCase()+' IN '+String(B.team.division||'DIVISION').split(' ').pop().toUpperCase():'');
  var ng=B.next_game;
  $('bugWhen').textContent=ng?((ng.home?'vs ':'at ')+((ng.opponent||{}).abbr||'')+'  '+fmtDate(ng.start_iso)+', '+fmtTime(ng.start_iso)):'TO BE ANNOUNCED';
  $('credit').textContent=B.copyright||'Official statistics provided by Maritime Hockey League. Powered by HockeyTech.com';
  tickCd();
}
function tickCd(){
  var ng=B&&B.next_game,el=$('bugCd');if(!ng){el.style.display='none';return;}
  var ms=Date.parse(ng.start_iso)-Date.now();
  if(isNaN(ms)){el.style.display='none';return;}
  el.style.display='';
  if(ms<=0){el.textContent='GAME DAY';return;}
  var s=Math.floor(ms/1000),d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60),sc=s%60;
  function p(x){return (x<10?'0':'')+x;}
  el.textContent=(d?d+'D ':'')+p(h)+':'+p(m)+':'+p(sc);
}

/* ---------- stage ---------- */
function ensureStage(){
  var st=$('stage'),dots=$('dots');
  if(panels.length)return;
  BUILD.forEach(function(b,i){var p=document.createElement('section');p.className='panel';st.appendChild(p);panels.push(p);var d=document.createElement('i');dots.appendChild(d);});
}
function fillAll(){BUILD.forEach(function(b,i){var html='';try{html=b[1]();}catch(e){html='';}panels[i].innerHTML=html;panels[i].dataset.empty=html?'':'1';});}
function show(i){
  var prev=cur>=0?panels[cur]:null;
  $('bugSec').textContent=BUILD[i][0];
  var ds=$('dots').children;for(var k=0;k<ds.length;k++)ds[k].className=k===i?'on':'';
  if(prev){prev.className='panel out';setTimeout(function(){if(prev!==panels[cur])prev.className='panel';},100);}
  panels[i].className='panel active';
  cur=i;
  var pr=$('prog');pr.className='';void pr.offsetWidth;pr.className='run';
}
function next(){
  var n=cur,tries=0;do{n=(n+1)%panels.length;tries++;}while(panels[n].dataset.empty&&tries<panels.length);
  if(cur<0){show(n);schedule();return;}
  var w=$('wipe');w.className='';void w.offsetWidth;w.className='go';
  setTimeout(function(){show(n);},480);
  setTimeout(function(){w.className='';},1100);
  schedule();
}
function schedule(){clearTimeout(timer);timer=setTimeout(next,DWELL);}

function load(first){
  return Promise.all([fetchJson('../../data/board.json'),fetchJson('../../data/insights.json')]).then(function(r){
    if(r[0])B=r[0];if(r[1])INS=r[1];
    if(!B)return;
    ensureStage();fillAll();buildBug();buildTicker();
    if(first){document.documentElement.style.setProperty('--dwell',DWELL+'ms');next();}
  });
}
load(true);
setInterval(function(){load(false);},300000);
cdT=setInterval(tickCd,1000);
})();
