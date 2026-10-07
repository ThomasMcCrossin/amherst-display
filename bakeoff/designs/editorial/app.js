(function(){
'use strict';
var TZ='America/Halifax', FRAME_MS=12000, REFRESH_MS=300000;
var B=null, INS=null, panels=[], cur=-1, timer=null;
var $=function(id){return document.getElementById(id)};
var NUMW=['zero','one','two','three','four','five','six','seven','eight','nine','ten'];
var ORD=['','first','second','third','fourth','fifth','sixth','seventh','eighth','ninth','tenth','11th','12th'];

function has(v){return v!==null&&v!==undefined&&v!==''&&!(typeof v==='number'&&isNaN(v))}
function txt(el,v){ if(!el)return; if(has(v)){el.textContent=String(v);el.hidden=false}else{el.textContent='';el.hidden=true} }
function img(el,url){ if(!el)return; if(url){ if(el.getAttribute('src')!==url)el.src=url; el.hidden=false; el.onerror=function(){el.hidden=true}; } else {el.removeAttribute('src');el.hidden=true} }
function fmtTime(d){return new Intl.DateTimeFormat('en-CA',{timeZone:TZ,hour:'numeric',minute:'2-digit',hour12:true}).format(d).replace(/\s?[ap]\.?m\.?/i,function(m){return ' '+(/a/i.test(m)?'AM':'PM')}).replace(/ /g,' ')}
function parts(d){var o={};new Intl.DateTimeFormat('en-US',{timeZone:TZ,weekday:'short',month:'short',day:'numeric',year:'numeric'}).formatToParts(d).forEach(function(p){o[p.type]=p.value});return o}
function fmtDate(d){var p=parts(d);return p.weekday+' '+p.month+' '+p.day}
function dayKey(d){var p=parts(d);return p.year+p.month+p.day}
function dayNum(d){var p=parts(d);return Date.UTC(+p.year,new Date(p.month+' 1, 2000').getMonth(),+p.day)/864e5}
function dateFromYMD(s){return new Date(s+'T12:00:00-03:00')}
function longDay(d){return new Intl.DateTimeFormat('en-US',{timeZone:TZ,weekday:'long'}).format(d)}
function num(v,dp){return has(v)&&isFinite(v)?Number(v).toFixed(dp):null}
function last(n){return n&&n.split(' ').slice(-1)[0]}
function lastName(n){if(!n)return '';var a=n.split(' ');return a.slice(1).join(' ')||a[0]}
function rec(s){return has(s)?s.replace(/-/g,'–'):null}
function heads(id){return id?'https://assets.leaguestat.com/mhl/240x240/'+id+'.jpg':null}
function nick(o){return o&&(o.nickname||o.name)}
function ordinal(n){return ORD[n]||(n+'th')}
function short(n){n=String(n||'');return n.split(' ').slice(-1)[0]}
function cap(s){return s?s.charAt(0).toUpperCase()+s.slice(1):s}

/* ---------- panels ---------- */
function setStats(id,arr){
  var kids=$(id).children;
  for(var i=0;i<kids.length;i++){
    var s=arr[i];
    if(s&&has(s[0])){kids[i].hidden=false;kids[i].children[0].textContent=s[0];kids[i].children[1].textContent=s[1]}
    else{kids[i].hidden=true}
  }
}
function setPerson(id,o){
  var el=$(id); if(!o){el.hidden=true;return} el.hidden=false;
  var ph=el.querySelector('.photo'); if(ph){ph.hidden=!o.img; img(ph.querySelector('img'),o.img)}
  var t=el.querySelector('.pt'); txt(t.children[0],o.k); txt(t.children[1],o.n); txt(t.children[2],o.s);
}

function renderNext(){
  var g=B.next_game; if(!g)return false;
  var o=g.opponent||{}, d=new Date(g.start_iso), now=new Date();
  var diff=dayNum(d)-dayNum(now);
  var when=diff===0?'Tonight':diff===1?'Tomorrow':diff>1&&diff<7?longDay(d):fmtDate(d);
  txt($('n-kicker'),diff===0?'Game day':'Next game · '+when);
  txt($('n-hl'),g.home?(nick(o)+' come to Amherst'):('Ramblers visit the '+(o.name||'road')));
  var dl=[fmtDate(d)+' · '+fmtTime(d)];
  if(g.venue)dl.push(g.venue);
  if(diff>1)dl.push('in '+diff+' days');
  var starter=null;
  if(g.official_starters){var s=g.home?g.official_starters.home:g.official_starters.away; var s2=g.home?g.official_starters.away:g.official_starters.home;
    var all=[]; if(g.official_starters.home)all.push(g.official_starters.home); if(g.official_starters.away)all.push(g.official_starters.away);
    starter=all.length?('Official starters: '+all.map(function(x){return x.name}).join(' and ')):null}
  txt($('n-deck'),dl.join(' · '));
  var sc=(o.leading_scorers||[])[0], pp=o.pp&&o.pp.percentage;
  setStats('n-stats',[
    [rec(o.record&&o.record.overall),(nick(o)||'Opponent')+' record'],
    [has(pp)?num(pp,1)+'%':null,'Power play'],
    [sc?String(sc.pts):null,'Points, '+(sc?lastName(sc.name):'')+' (their leader)']
  ]);
  txt($('n-note'),starter||'Starter TBA');
  img($('n-logo-me'),B.team&&B.team.logo_url); img($('n-logo-opp'),o.logo_url);
  setPerson('n-person',sc?{img:sc.headshot_url,k:(o.abbr||'')+' top scorer',n:sc.name,s:sc.g+' G · '+sc.a+' A · '+sc.pts+' PTS'}:null);
  return true;
}

function renderInsight(){
  var items=INS&&!INS.stub&&INS.items&&INS.items.slice().sort(function(a,b){return (a.priority||99)-(b.priority||99)});
  if(!items||!items.length)return false;
  var it=items[0];
  txt($('i-kicker'),cap(it.kind||'Insight')+' · Stat of the day');
  txt($('i-hl'),it.headline); txt($('i-deck'),it.detail);
  var sk=(B.skaters||[]).filter(function(s){return it.player_id&&s.id===it.player_id})[0];
  var ph=$('i-photo'); var u=it.player_id?heads(it.player_id):null; ph.hidden=!u; img(ph.querySelector('img'),u);
  var bn=$('i-num'); if(it.stat&&has(it.stat.value)){bn.hidden=false;bn.children[0].textContent=it.stat.value;bn.children[1].textContent=it.stat.label||''}else bn.hidden=true;
  return true;
}

function renderResult(){
  var r=(B.recent||[])[0]; if(!r||!r.score)return false;
  var o=r.opp||{}, d=dateFromYMD(r.date), ot=r.ot_so?(r.ot_so==='SO'?' in a shootout':' in overtime'):'';
  var f=r.score['for'],a=r.score.against,w=r.result==='W';
  var verb=w?(f-a>=3?'rout':'beat'):(r.result==='L'?'fall to':'lose to');
  var on=o.name?('the '+(o.nickname||o.name.split(' ').slice(-1)[0])):'the opposition';
  var hl=w?('Ramblers '+verb+' '+on+' '+f+'–'+a+(r.ot_so?(' ('+r.ot_so+')'):'')):('Ramblers '+verb+' '+o.name+', '+a+'–'+f+(r.ot_so?(' ('+r.ot_so+')'):''));
  txt($('r-kicker'),'Last time out · '+fmtDate(d));
  txt($('r-hl'),hl);
  var sc=(r.scorers||[]);
  var dk=sc.length?'Ramblers scoring: '+sc.map(function(x){return x.scorer+' ('+x.period+')'}).join(', ')+'.':'';
  txt($('r-deck'),dk||((r.home?'At home':'On the road')+(ot?ot:'')+'.'));
  var sh=r.shots;
  setStats('r-stats',[
    [sh&&has(sh['for'])?sh['for']+'–'+sh.against:null,'Shots, Ramblers–'+(o.abbr||'opp')],
    [r.pp&&r.pp['for']?r.pp['for']:null,'Power-play goals/chances'],
    [has(r.attendance)&&r.attendance>0?String(r.attendance):null,'In the building']
  ]);
  var fm=$('r-form'); var rs=(B.recent||[]).slice(0,5).reverse();
  while(fm.children.length<6){var e=document.createElement('span');fm.appendChild(e)}
  fm.children[0].className='lbl'; fm.children[0].textContent='Last '+rs.length;
  for(var i=1;i<6;i++){var c=fm.children[i];var x=rs[i-1];if(x){c.className='chip '+x.result;c.textContent=x.result;c.hidden=false}else c.hidden=true}
  var st=r.three_stars||[]; var ids=['r-s1','r-s2','r-s3'];
  ids.forEach(function(id,i){var s=st[i];
    if(!s){$(id).hidden=true;return}
    var tm=s.team||''; var ln=has(s.line)?s.line:null;
    setPerson(id,{img:i===0?heads(s.id):null,k:(i+1===1?'First':i+1===2?'Second':'Third')+' star'+(tm?' · '+tm:''),n:s.name,s:ln});
  });
  return true;
}

function renderPlayer(){
  var sk=(B.skaters||[]); if(!sk.length)return false;
  var p=sk[0], rk=B.league_leaders&&B.league_leaders.ramblers_ranks;
  var gr=rk&&rk.goals&&rk.goals.filter(function(x){return x.id===p.id})[0];
  var pr=rk&&rk.points&&rk.points.filter(function(x){return x.id===p.id})[0];
  txt($('p-kicker'),'Ramblers scoring leader');
  txt($('p-hl'),lastName(p.name)+' leads the Ramblers with '+p.pts+' point'+(p.pts===1?'':'s'));
  var bits=[p.pos?(p.pos==='C'?'Centre':p.pos==='D'?'Defence':p.pos==='G'?'Goalie':'Winger'):null,has(p.number)?'#'+p.number:null,p.hometown].filter(Boolean);
  var dk=p.gp+' games played.'; if(pr)dk+=' '+ordinal(pr.rank).replace(/^./,function(c){return c.toUpperCase()})+' in the MHL in points'+(gr?', '+ordinal(gr.rank)+' in goals':'')+'.';
  txt($('p-deck'),dk);
  setStats('p-stats',[[String(p.pts),'Points'],[String(p.g),'Goals'],[String(p.a),'Assists']]);
  var ms=(B.milestones_near||[])[0];
  txt($('p-note'),ms?(function(){var u=ms.stat.replace(/^career MHL /,'').replace(/s$/,'');return ms.name+' is '+ms.needs+' '+u+(ms.needs===1?'':'s')+' from '+ms.milestone+' career '+u+'s.'})():'');
  var ph=$('p-photo'); ph.hidden=!p.headshot_url; img(ph.querySelector('img'),p.headshot_url);
  var li=$('p-list').children;
  for(var i=0;i<li.length;i++){var s=sk[i+1];
    if(s){li[i].hidden=false;li[i].innerHTML='';var n=document.createElement('span');n.textContent=s.name;var b=document.createElement('b');b.textContent=s.pts+' pts';li[i].appendChild(n);li[i].appendChild(b)}else li[i].hidden=true}
  return true;
}

function logoCell(td,url,me){
  td.className='lg'; td.textContent='';
  if(!url)return;
  var d=document.createElement('div');d.className='disc';var im=document.createElement('img');im.alt='';im.src=url;im.onerror=function(){d.hidden=true};d.appendChild(im);td.appendChild(d);
}
function renderStandings(){
  var divs=(B.standings&&B.standings.divisions)||[];
  var div=divs.filter(function(d){return d.rows.some(function(r){return r.is_ramblers})})[0]; if(!div)return false;
  var rows=div.rows.slice(0,6), me=rows.filter(function(r){return r.is_ramblers})[0]||div.rows.filter(function(r){return r.is_ramblers})[0];
  var third=div.rows[2];
  var h;
  if(me.rank===1)h='Ramblers lead the '+div.name.replace('Eastlink ','')+' with '+me.pts+' points';
  else{
    var ahead=div.rows[me.rank-2], gap=ahead.pts-me.pts;
    h=cap(ordinal(me.rank))+' in the '+div.name.replace('Eastlink ','')+', '+(gap===0?'level with ':NUMW[gap]?NUMW[gap]+' point'+(gap===1?'':'s')+' behind ':gap+' points behind ')+ahead.abbr;
  }
  txt($('s-kicker'),div.name+' division · Standings');
  txt($('s-hl'),h);
  var tb=$('s-tbl').tBodies[0];
  while(tb.rows.length<rows.length){var tr=tb.insertRow();for(var k=0;k<9;k++)tr.insertCell()}
  while(tb.rows.length>rows.length)tb.deleteRow(-1);
  var po=(B.standings.playoff_format||'');
  rows.forEach(function(r,i){var tr=tb.rows[i],c=tr.cells;
    tr.className=(r.is_ramblers?'me ':'')+(r.rank===4&&rows.length>4?'cut':'');
    c[0].className='rk';c[0].textContent=r.rank;
    logoCell(c[1],r.logo_url);
    c[2].className='l';c[2].textContent=r.abbr+' · '+String(r.team).replace(/^.*?\s(?=\S+$)/,function(m){return r.team.length>16?'':m});
    c[2].textContent=short(r.team);
    var vals=[r.gp,r.w,r.l,(r.otl||0)+(r.sol||0),has(r.diff)?(r.diff>0?'+'+r.diff:r.diff):null,r.pts];
    for(var j=0;j<6;j++){c[3+j].className=j===5?'pts':'';c[3+j].textContent=has(vals[j])?vals[j]:''}
  });
  var ts=B.team_stats||{}, lr=ts.league_ranks||{};
  setStats('s-stats',[
    [has(ts.pk_pct)?num(ts.pk_pct,1)+'%':null,'Penalty kill'+(lr.pk_pct?', '+ordinal(lr.pk_pct)+' of '+lr.of_teams:'')],
    [has(ts.gf_pg)?num(ts.gf_pg,2):null,'Goals per game'+(lr.gf?', '+ordinal(lr.gf)+' of '+lr.of_teams:'')],
    [has(ts.pp_pct)?num(ts.pp_pct,1)+'%':null,'Power play'+(lr.pp_pct?', '+ordinal(lr.pp_pct)+' of '+lr.of_teams:'')]
  ]);
  return true;
}

function renderSchedule(){
  var up=(B.upcoming||[]).slice(0,6); if(!up.length)return false;
  var home=up.filter(function(g){return g.home}).length, away=up.length-home;
  var first=new Date(up[0].start_iso), lastD=new Date(up[up.length-1].start_iso);
  var span=dayNum(lastD)-dayNum(first)+1;
  txt($('c-kicker'),'Coming up · Schedule');
  txt($('c-hl'),cap(NUMW[up.length]||up.length)+' games in '+span+' days, '+(NUMW[home]||home)+' at home');
  var list=$('c-list');
  while(list.children.length<up.length){var r=document.createElement('div');r.className='row';
    r.innerHTML='<div class="d"></div><div class="disc"><img alt=""></div><div class="o"></div><div class="t"></div>';list.appendChild(r)}
  for(var i=0;i<list.children.length;i++){var r=list.children[i],g=up[i];
    if(!g){r.hidden=true;continue}
    r.hidden=false;var d=new Date(g.start_iso),o=g.opp||{};
    r.children[0].textContent=fmtDate(d);
    img(r.children[1].firstChild,o.logo_url);
    var oc=r.children[2];oc.textContent='';var sm=document.createElement('small');sm.textContent=g.home?'VS':'AT';oc.appendChild(sm);oc.appendChild(document.createTextNode(short(o.name)));
    r.children[3].textContent=fmtTime(d);
  }
  setStats('c-stats',[[String(home),'Home games at Amherst Stadium'],[String(away),'Games on the road']]);
  return true;
}

/* ---------- rotation ---------- */
var RENDER={next:renderNext,insight:renderInsight,result:renderResult,player:renderPlayer,standings:renderStandings,schedule:renderSchedule};
var ALL=[].slice.call(document.querySelectorAll('.panel'));
function build(){
  var was=cur>=0&&panels[cur]?panels[cur].dataset.key:null;
  panels=ALL.filter(function(p){var ok=false;try{ok=RENDER[p.dataset.key]()}catch(e){console.error('render '+p.dataset.key,e)}return ok});
  var dots=$('dots'); while(dots.children.length<panels.length)dots.appendChild(document.createElement('i'));
  while(dots.children.length>panels.length)dots.removeChild(dots.lastChild);
  var idx=was?panels.findIndex(function(p){return p.dataset.key===was}):-1;
  cur=idx>=0?idx:0; show();
}
function show(){
  ALL.forEach(function(p){p.classList.toggle('on',p===panels[cur])});
  for(var i=0;i<$('dots').children.length;i++)$('dots').children[i].className=i===cur?'on':'';
}
function tick(){ if(panels.length){cur=(cur+1)%panels.length;show()} }

function load(){
  var bust='?t='+Date.now();
  var pb=fetch('../../data/board.json'+bust).then(function(r){if(!r.ok)throw new Error('board '+r.status);return r.json()});
  var pi=fetch('../../data/insights.json'+bust).then(function(r){return r.ok?r.json():null}).catch(function(){return null});
  return Promise.all([pb,pi]).then(function(v){B=v[0];INS=v[1];
    txt($('copy'),B.copyright||'Official statistics provided by Maritime Hockey League. Powered by HockeyTech.com');
    txt($('mast-date'),fmtDate(new Date()));
    build();
  }).catch(function(e){console.error('load failed',e)});
}
load().then(function(){ timer=setInterval(tick,FRAME_MS); });
setInterval(load,REFRESH_MS);
})();
