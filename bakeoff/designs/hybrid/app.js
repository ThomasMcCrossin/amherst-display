(function(){
'use strict';
var DWELL=12000, TZ='America/Halifax', REFRESH=300000, MAXP=7;
var qs=new URLSearchParams(location.search), override=qs.get('now'), offset=0;
if(override){var t0=Date.parse(override);if(!isNaN(t0))offset=t0-Date.now();else override=null;}
function now(){return Date.now()+offset;}
var B=null, INS=null, mode=null, seq=[], panels=[], dots=[], cur=-1, timer=0, lastSig='', cdEls=[], storyPage=0, tickSig='', cycle=0, fixture=(qs.get('fixture')||'').match(/^[\w.\-]+\.json$/)?qs.get('fixture'):null;
var $=function(id){return document.getElementById(id);};
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});}
function has(v){return v!==null&&v!==undefined&&v!==''&&!(typeof v==='number'&&isNaN(v));}
function num(v,d){return has(v)&&isFinite(+v)?(+v).toFixed(d==null?0:d):'';}
function pct3(v){return has(v)&&isFinite(+v)?(+v).toFixed(3).replace(/^0/,''):'';}
function ord(n){n=+n;if(!n||!isFinite(n))return '';var s=['th','st','nd','rd'],v=n%100;return n+(s[(v-20)%10]||s[v]||s[0]);}

/* ---------- NOTABILITY GATE ----------
   One rule for every rank or badge on screen. A mediocre rank is dropped, never softened. */
var GATE={ind:10,rookie:5,team:3,div:2};
function notable(kind,rank){rank=+rank;return !!rank&&isFinite(rank)&&rank>0&&rank<=(GATE[kind]||0);}

function img(u,cls){return has(u)?'<img'+(cls?' class="'+cls+'"':'')+' src="'+esc(u)+'" alt="" onerror="this.style.visibility=\'hidden\'">':'';}
function disc(u){return '<div class="disc">'+img(u)+'</div>';}
function nb(s){return String(s).replace(/[  ]/g,' ');}
var dfDate=new Intl.DateTimeFormat('en-CA',{timeZone:TZ,year:'numeric',month:'2-digit',day:'2-digit'});
var dfParts=new Intl.DateTimeFormat('en-US',{timeZone:TZ,weekday:'short',month:'short',day:'numeric'});
var dfTime=new Intl.DateTimeFormat('en-US',{timeZone:TZ,hour:'numeric',minute:'2-digit',hour12:true});
var dfHour=new Intl.DateTimeFormat('en-US',{timeZone:TZ,hour:'numeric',hour12:false});
function dkey(ms){return dfDate.format(new Date(ms));}
function dnice(ms){var p={};dfParts.formatToParts(new Date(ms)).forEach(function(x){p[x.type]=x.value;});return p.weekday+' '+p.month+' '+p.day;}
function tnice(ms){return nb(dfTime.format(new Date(ms)));}
function dayNum(k){var a=k.split('-');return Math.round(Date.UTC(+a[0],+a[1]-1,+a[2])/864e5);}
function niceKey(k){return k?dnice(Date.parse(k+'T15:00:00-03:00')):'';}
function trunc(s,max){s=String(s==null?'':s).trim();if(s.length<=max)return s;var c=s.slice(0,max),i=c.lastIndexOf(' ');if(i>max*0.5)c=c.slice(0,i);return c.replace(/[\s,;:.\-]+$/,'')+'…';}
function fetchJson(u){return fetch(u+'?t='+Date.now(),{cache:'no-store'}).then(function(r){if(!r.ok)throw new Error(r.status);return r.json();}).catch(function(){return null;});}
function streak(s){var m=String(s||'').split('-').map(Number);if(m.length!==4||m.some(isNaN))return '';var l=m[1]+m[2]+m[3];if(m[0]>0&&!l)return 'W'+m[0];if(!m[0]&&l)return 'L'+l;return '';}
function rcls(r){return r==='W'?'W':(r==='L'?'L':'O');}
function rlab(r){return r==='W'?'W':(r==='L'?'L':'OT');}
function recStr(r){return r?r.w+'-'+r.l+'-'+(+r.otl||0)+'-'+(+r.sol||0):'';}

/* ---------- schedule + calendar mode (from Gameday) ---------- */
function games(){
  var out=[];
  (B.recent||[]).forEach(function(g){out.push({id:g.id,ms:Date.parse(g.start_iso),home:g.home,opp:g.opp||{},venue:g.venue,final:true,rec:g});});
  (B.upcoming||[]).forEach(function(g){out.push({id:g.id,ms:Date.parse(g.start_iso),home:g.home,opp:g.opp||{},venue:g.venue,final:false});});
  out=out.filter(function(g){return !isNaN(g.ms);});
  out.sort(function(a,b){return a.ms-b.ms;});return out;
}
function pickMode(){
  var t=now(),today=dkey(t),G=games(),i,g,td=null,yd=null,ydn=dayNum(today)-1;
  for(i=0;i<G.length;i++){g=G[i];var k=dkey(g.ms);if(k===today)td=g;if(g.final&&dayNum(k)===ydn)yd=g;}
  if(td&&td.final)return {key:'recap',game:td,label:'FINAL'};
  if(td)return {key:td.home?'home':'away',game:td,label:td.home?'HOME GAME DAY':'AWAY GAME DAY'};
  if(yd)return {key:'recap',game:yd,label:'LAST NIGHT'};
  return {key:'off',game:null,label:'OFF DAY'};
}
function nextUp(after){var G=games();for(var i=0;i<G.length;i++)if(!G[i].final&&G[i].ms+3*3600e3>after)return G[i];return null;}
function fullNG(g){var n=B.next_game;return n&&g&&n.id===g.id?n:null;}
function srow(id){var D=(B.standings||{}).divisions||[];for(var i=0;i<D.length;i++)for(var j=0;j<D[i].rows.length;j++)if(D[i].rows[j].team_id===id)return {r:D[i].rows[j],d:D[i].name};return null;}
function myDiv(){var D=((B.standings||{}).divisions)||[];for(var i=0;i<D.length;i++)for(var j=0;j<D[i].rows.length;j++)if(D[i].rows[j].is_ramblers)return D[i];return D[0]||null;}
function divWord(n){return String(n||'').split(' ').pop().toUpperCase();}
function divText(rank,name){return notable('div',rank)?ord(rank).toUpperCase()+' IN '+divWord(name):String(name||'').toUpperCase();}

/* ---------- insights (parent already gates; skip notable:false and expired) ---------- */
function realInsights(){
  if(!INS||INS.stub||!INS.items)return [];
  var t=now();
  return INS.items.filter(function(i){
    if(!i||!i.headline||/^stub/.test(i.id||'')||i.notable===false)return false;
    if(i.valid_until){var v=Date.parse(i.valid_until);if(!isNaN(v)&&v<t)return false;}
    return true;
  }).sort(function(a,b){return (a.priority||9)-(b.priority||9);});
}
function factItems(){
  var out=[],ts=B.team_stats||{},lr=ts.league_ranks||{},n=lr.of_teams||'';
  var sk=(B.skaters||[])[0];
  if(sk&&has(sk.pts)){var rk=(((B.league_leaders||{}).ramblers_ranks||{}).points||[]).filter(function(x){return x.id===sk.id;})[0];
    out.push({kind:'TEAM LEADER',headline:sk.name+' leads the Ramblers in scoring',detail:sk.g+' goals, '+sk.a+' assists in '+sk.gp+' games'+(rk&&notable('ind',rk.rank)?' - '+ord(rk.rank)+' in the MHL':''),stat:{value:String(sk.pts),label:'POINTS'}});}
  if(has(ts.pk_pct)&&notable('team',lr.pk_pct))out.push({kind:'SPECIAL TEAMS',headline:'Penalty kill ranks '+ord(lr.pk_pct)+(n?' of '+n:''),detail:'Killing '+num(ts.pk_pct,1)+'% of penalties',stat:{value:num(ts.pk_pct,1)+'%',label:'PENALTY KILL'}});
  if(has(ts.pp_pct)&&notable('team',lr.pp_pct))out.push({kind:'SPECIAL TEAMS',headline:'Power play ranks '+ord(lr.pp_pct)+(n?' of '+n:''),detail:'Scoring on '+num(ts.pp_pct,1)+'% of chances',stat:{value:num(ts.pp_pct,1)+'%',label:'POWER PLAY'}});
  var m=(B.milestones_near||[])[0];
  if(m)out.push({kind:'MILESTONE WATCH',headline:m.name+' is '+m.needs+' away from '+m.milestone,detail:String(m.stat||'').replace(/^career /,'Career ')+': '+m.current+' so far, '+m.milestone+' is next',stat:{value:String(m.needs),label:'TO GO'}});
  if(has(ts.shots_for_pg))out.push({kind:'SHOTS',headline:'Ramblers average '+num(ts.shots_for_pg,1)+' shots a game',detail:'Opponents average '+num(ts.shots_against_pg,1)+' (last '+ts.shots_sample_games+' games)',stat:{value:num(ts.shots_for_pg,1),label:'SHOTS / GAME'}});
  return out;
}
function storyCards(){
  var all=realInsights().map(function(i){return {kind:String(i.kind||'STORYLINE').toUpperCase(),headline:i.headline,detail:i.detail,stat:i.stat&&has(i.stat.value)?i.stat:null};});
  if(all.length<3)all=all.concat(factItems());
  var n=all.length,out=[];if(!n)return out;
  for(var k=0;k<Math.min(3,n);k++)out.push(all[(storyPage*3+k)%n]);
  return out;
}

/* ---------- shared pieces ---------- */
function hd(a,b){return '<div class="hd rise"><div class="slab">'+esc(a)+'</div>'+(b?'<div class="sub">'+esc(b)+'</div>':'')+'</div>';}
function formPills(arr){return arr.map(function(r){return '<div class="pill '+rcls(r)+'">'+rlab(r)+'</div>';}).join('');}
function myForm(){return (B.recent||[]).slice(0,5).reverse().map(function(g){return g.result;});}
function oppForm(n){return n&&n.opponent_form&&n.opponent_form.last5?n.opponent_form.last5.slice().reverse().map(function(g){return g.result;}):[];}

/* team view of a matchup: full data if this is B.next_game, else standings-derived */
function teams(g){
  var n=fullNG(g),opp=g.opp||{},o=n?n.opponent:null,sr=srow(opp.id),os=sr?sr.r:(o&&o.standings)||{},ts=B.team_stats||{},ms=srow(B.team.id),gp=+os.gp||0;
  var us={name:'AMHERST RAMBLERS',logo:B.team.logo_url,div:divText(ms?ms.r.rank:ts.division_rank,B.team.division),rec:ts.record,pct:ts.pct,gf:ts.gf_pg,ga:ts.ga_pg,pp:ts.pp_pct,pk:ts.pk_pct,form:myForm(),cls:'us'};
  var th={name:String(opp.name||'').toUpperCase(),logo:opp.logo_url,div:divText(os.rank,(sr&&sr.d)||opp.division||''),
    rec:(o&&o.record&&o.record.overall)||recStr(os),pct:os.pct,gf:gp&&has(os.gf)?os.gf/gp:null,ga:gp&&has(os.ga)?os.ga/gp:null,
    pp:o&&o.pp&&has(o.pp.percentage)?o.pp.percentage:os.pp_pct,pk:o&&o.pk&&has(o.pk.percentage)?o.pk.percentage:os.pk_pct,form:oppForm(n),cls:'them'};
  return {us:us,th:th,n:n};
}

/* ---------- panels ---------- */
function tapeHtml(g){
  if(!g||!B.team)return '';
  var T=teams(g),us=T.us,th=T.th,n=T.n,opp=g.opp||{};
  var L=g.home?th:us,R=g.home?us:th;
  function side(t,tag){return '<div class="side '+t.cls+' rise">'+disc(t.logo)+'<div class="tag">'+tag+'</div><div class="tn">'+esc(t.name)+'</div><div class="tr">'+esc(t.div)+'</div>'+(t.form.length?'<div class="form"><span class="fl">LAST '+t.form.length+'</span>'+formPills(t.form)+'</div>':'')+'</div>';}
  var defs=[['RECORD','rec',0,0],['POINTS %','pct',3,1],['GOALS FOR / GM','gf',2,1],['GOALS AGAINST / GM','ga',2,-1],['POWER PLAY','pp',1,1],['PENALTY KILL','pk',1,1]],rows='',k=0;
  defs.forEach(function(d){
    var key=d[1];
    function f(t){var v=t[key];if(!has(v)||v==='')return '--';if(key==='rec')return String(v);if(key==='pct')return pct3(v);return num(v,d[2])+(key==='pp'||key==='pk'?'%':'');}
    var a=L[key],b=R[key],la='',lb='';
    if(f(L)==='--'&&f(R)==='--')return;
    if(d[3]!==0&&has(a)&&has(b)&&+a!==+b){var aw=(+a>+b)===(d[3]>0);la=aw?' lead':'';lb=aw?'':' lead';}
    k++;
    rows+='<div class="row rise" style="--i:'+k+'"><div class="v'+la+'">'+esc(f(L))+'</div><div class="lab">'+d[0]+'</div><div class="v'+lb+'">'+esc(f(R))+'</div></div>';
  });
  var gtxt='Starter TBA',h2h='';
  if(n){
    var st=n.official_starters||{},sh=st.home&&st.home.name,sa=st.away&&st.away.name;
    if(sh||sa){var parts=[];if(sa)parts.push((n.home?opp.abbr:'AMH')+' '+sa);if(sh)parts.push((n.home?'AMH':opp.abbr)+' '+sh);gtxt=parts.join('  /  ');}
    var hh=(n.h2h_this_season&&n.h2h_this_season.length)?n.h2h_this_season:(n.h2h_last_season||[]),hl=(n.h2h_this_season&&n.h2h_this_season.length)?'HEAD TO HEAD THIS SEASON':'HEAD TO HEAD LAST SEASON';
    h2h=hh.length?hh.slice(0,3).map(function(x){return x.result+' '+x.score['for']+'-'+x.score.against+(x.home?' home':' away');}).join('   |   '):'First meeting of the season';
    h2h='<div class="tf"><div class="k">'+hl+'</div><div class="t">'+esc(h2h)+'</div></div>';
  }else{
    var tr=(B.recent||[]).filter(function(r){return r.opp&&r.opp.id===opp.id;});
    h2h='<div class="tf"><div class="k">HEAD TO HEAD THIS SEASON</div><div class="t">'+esc(tr.length?tr.slice(0,3).map(function(x){return x.result+' '+x.score['for']+'-'+x.score.against+(x.home?' home':' away');}).join('   |   '):'No games yet')+'</div></div>';
  }
  return hd('TALE OF THE TAPE',dnice(g.ms)+' · '+tnice(g.ms)+' · '+String(g.venue||'').toUpperCase())+
   '<div class="tape">'+side(L,'AWAY')+'<div class="mid">'+rows+'</div>'+side(R,'HOME')+'</div>'+
   '<div class="tfoot rise" style="--i:8"><div class="tf"><div class="k">STARTING GOALIE</div><div class="t">'+esc(gtxt)+'</div></div>'+h2h+'</div>';
}
function heroSide(t,tag){return '<div class="hs2 '+t.cls+' rise">'+disc(t.logo)+'<div class="tag">'+tag+'</div><div class="tn">'+esc(t.name)+'</div><div class="rec">'+esc(t.rec||'')+'</div><div class="tr">'+esc(t.div)+'</div>'+(t.form.length?'<div class="form"><span class="fl">LAST '+t.form.length+'</span>'+formPills(t.form)+'</div>':'')+'</div>';}
function tonightWord(g){var h=+dfHour.format(new Date(g.ms));return dkey(now())===dkey(g.ms)?(h>=17?'TONIGHT':'TODAY'):dnice(g.ms).toUpperCase();}
function heroHome(m){
  var g=m.game,T=teams(g);
  return '<div class="hero">'+heroSide(T.us,'HOME')+'<div class="hc rise" style="--i:2"><div class="eyebrow">HOME GAME DAY</div><div class="when">'+tonightWord(g)+' · '+tnice(g.ms)+'</div><div class="cdbig cd" data-ms="'+g.ms+'"></div><div class="until">UNTIL PUCK DROP</div><div class="venue">'+esc(String(g.venue||'Amherst Stadium').toUpperCase())+'</div></div>'+heroSide(T.th,'VISITORS')+'</div>';
}
function heroAway(m){
  var g=m.game,T=teams(g),n=T.n;
  return '<div class="hero">'+heroSide(T.us,'VISITORS')+'<div class="hc rise" style="--i:2"><div class="eyebrow gold">ROAD GAME</div><div class="when">'+tonightWord(g)+' · '+tnice(g.ms)+'</div><div class="cdbig mid cd" data-ms="'+g.ms+'"></div><div class="until">UNTIL PUCK DROP</div><div class="flo"><div class="a">WATCH LIVE ON FLOHOCKEY</div><div class="b">'+(n&&n.flo_url?'Search Amherst Ramblers on flohockey.tv':'Find the Ramblers game on flohockey.tv')+'</div></div><div class="venue">AT '+esc(String(g.venue||'').toUpperCase())+'</div></div>'+heroSide(T.th,'HOME')+'</div>';
}
function lastHtml(m){
  var g=m.game.rec;if(!g)return '';
  var win=g.result==='W',ot=g.ot_so?(' / '+g.ot_so):'',o=g.opp||{};
  function tm(abbr,logo,sc,w){return '<div class="tm">'+disc(logo)+'<div class="a">'+esc(abbr)+'</div><div class="s '+(w?'w':'l')+'">'+esc(sc)+'</div></div>';}
  var us=tm('AMH',B.team.logo_url,g.score['for'],win),th=tm(o.abbr,o.logo_url,g.score.against,!win);
  var chips=(B.recent||[]).slice(0,5).map(function(r){return '<div class="chip '+rcls(r.result)+'"><b>'+r.score['for']+'-'+r.score.against+'</b><span>'+(r.home?'vs ':'@ ')+esc((r.opp||{}).abbr)+'</span></div>';}).join('');
  var stars=(g.three_stars||[]).slice(0,3).map(function(s,i){return '<div class="star rise" style="--i:'+(i+1)+'"><div class="ph">'+img('https://assets.leaguestat.com/mhl/240x240/'+s.id+'.jpg')+'</div><div class="sn">&#9733; '+s.star+'</div><div class="nm">'+esc(s.name)+'</div><div class="ln">'+esc(s.team)+' &middot; '+esc(trunc(s.line||'',22))+'</div></div>';}).join('');
  var gl=(g.goals||[]).slice(0,5).map(function(x){return '<div class="g'+(x.mine?' mine':'')+'"><span class="tm0">'+esc(x.team)+'</span><span class="pd">'+esc(x.period)+' '+esc(x.time)+'</span><span class="who">'+esc(x.scorer)+(x.assists&&x.assists.length?'<small>'+esc(trunc(x.assists.join(', '),34))+'</small>':'')+(x.pp?'<small>PP</small>':'')+'</span></div>';}).join('');
  var left=g.home?th:us,right=g.home?us:th; /* visitors on the left, as on the tape */
  return hd('FINAL',niceKey(g.date).toUpperCase()+' · '+(g.home?'AT HOME':'ON THE ROAD'))+'<div class="lg"><div class="sc rise"><div class="fin">FINAL'+esc(ot)+'</div><div class="fin2">'+esc(String(g.venue||'').toUpperCase())+(has(g.attendance)?' &middot; '+esc(g.attendance)+' FANS':'')+'</div><div class="teams">'+left+right+'</div><div class="res5"><div class="rh">LAST 5 RESULTS</div><div class="chips">'+chips+'</div></div></div>'+
   '<div class="rt">'+(stars?'<div class="stars">'+stars+'</div>':'')+(gl?'<div class="goals rise" style="--i:4"><div class="gh">SCORING</div>'+gl+'</div>':'')+'</div></div>';
}
function standingsHtml(){
  var d=myDiv();if(!d)return '';
  var rows=d.rows.slice(0,7);
  var h=hd('STANDINGS',String(d.name||'').toUpperCase()+' DIVISION')+'<div class="tbl"><div class="tr0 rise"><span>RK</span><span></span><span>TEAM</span><span>GP</span><span>W</span><span>L</span><span>OTL</span><span>STREAK</span><span>PTS</span></div>';
  rows.forEach(function(r,i){var ot=(+r.otl||0)+(+r.sol||0);
    h+='<div class="tr1 rise'+(r.is_ramblers?' us':'')+'" style="--i:'+(i+1)+'"><span class="rk">'+esc(r.rank)+'</span>'+disc(r.logo_url)+'<span class="nm">'+esc(r.team)+'</span><span>'+esc(r.gp)+'</span><span>'+esc(r.w)+'</span><span>'+esc(r.l)+'</span><span>'+ot+'</span><span class="st">'+streak(r.streak)+'</span><span class="pt">'+esc(r.pts)+'</span></div>';});
  return h+'</div>';
}
function leadersHtml(){
  var sk=(B.skaters||[]).slice(0,4);if(!sk.length)return '';
  var rk=(((B.league_leaders||{}).ramblers_ranks||{}).points)||[];
  var h=hd('RAMBLERS LEADERS','TEAM SCORING · '+(B.season||''))+'<div class="lead4">';
  sk.forEach(function(p,i){var r=rk.filter(function(x){return x.id===p.id;})[0];
    h+='<div class="lc rise" style="--i:'+i+'"><div class="ph">'+img(p.headshot_url)+(has(p.number)?'<div class="no">#'+esc(p.number)+'</div>':'')+(r&&notable('ind',r.rank)?'<div class="rkb">'+ord(r.rank)+' IN MHL</div>':'')+'</div><div class="nm">'+esc(p.name)+'</div><div class="ps">'+esc(p.pos||'')+(p.gp?' &middot; '+esc(p.gp)+' GP':'')+'</div><div class="big">'+esc(p.pts)+'<small>PTS</small></div><div class="ga">'+esc(p.g)+' G &nbsp;&nbsp; '+esc(p.a)+' A</div></div>';});
  h+='</div><div class="gstrip">';
  var lg=((B.league_leaders||{}).goalies)||{};
  function grank(list,id){for(var i=0;i<(list||[]).length;i++)if(list[i].id===id)return i+1;return 0;}
  (B.goalies||[]).filter(function(g){return g.gp;}).slice(0,2).forEach(function(g,i){
    var rs=grank(lg.sv_pct,g.id),tag=notable('ind',rs)?' · '+ord(rs).toUpperCase()+' IN MHL SV%':'';
    h+='<div class="gs rise" style="--i:'+(i+4)+'">'+img(g.headshot_url)+'<div><div class="k">GOALIE'+tag+'</div><div class="n">'+esc(g.name)+'</div></div><div class="nums"><div>'+(has(g.sv_pct)?pct3(g.sv_pct):'--')+'<small>SV%</small></div><div>'+(has(g.gaa)?num(g.gaa,2):'--')+'<small>GAA</small></div><div>'+esc(g.w||0)+'-'+esc(g.l||0)+'<small>W-L</small></div></div></div>';});
  return h+'</div>';
}
function streaksHtml(){
  var sk=((B.streaks&&B.streaks.ramblers)||[]).slice(0,4).map(function(s){var k=s.kind==='goals'?'goal streak':s.kind==='points'?'point streak':String(s.kind)+' streak';
    return '<div class="ln2"><div class="gw">'+esc(s.name)+'<small>'+esc(s.games)+'-game '+esc(k)+(s.active?' (active)':'')+'</small></div><div class="gv">'+(s.kind==='goals'?esc(s.g)+'G':esc(s.pts)+'P')+'</div></div>';}).join('')||'<div class="none">No streaks to report</div>';
  var ms=(B.milestones_near||[]).slice(0,4).map(function(x){
    return '<div class="ln2"><div class="gw">'+esc(x.name)+'<small>'+esc(x.needs)+' '+(x.needs===1?'away from':'to')+' '+esc(x.milestone)+' '+esc(String(x.stat||'').replace('career MHL ','career '))+'</small></div><div class="gv">'+esc(x.current)+'</div></div>';}).join('')||'<div class="none">No milestones this close</div>';
  return hd('RUNS & MILESTONES','WHO IS HOT, WHO IS CLOSE')+'<div class="two rise" style="--i:1"><div class="cd2"><h3>RUNS THIS SEASON</h3>'+sk+'</div><div class="cd2"><h3>MILESTONES WITHIN REACH</h3>'+ms+'</div></div>';
}
function schedHtml(){
  var up=(B.upcoming||[]).filter(function(g){return Date.parse(g.start_iso)+3*3600e3>now();}).slice(0,5),ts=B.team_stats||{},lr=ts.league_ranks||{},n=lr.of_teams?' OF '+lr.of_teams:'';
  if(!up.length)return '';
  var h=hd('COMING UP','SCHEDULE & BY THE NUMBERS')+'<div class="sched"><div class="sl">';
  up.forEach(function(g,i){var o=g.opp||{},ms=Date.parse(g.start_iso);
    h+='<div class="sr rise'+(i===0?' first':'')+'" style="--i:'+i+'"><div class="dd">'+esc(dnice(ms).toUpperCase())+'<small>'+esc(tnice(ms))+'</small></div>'+disc(o.logo_url)+'<div class="on">'+esc(String(o.name||'').toUpperCase())+'<small>'+esc(String(g.venue||'').toUpperCase())+'</small></div><div class="ha'+(g.home?'':' away')+'">'+(g.home?'HOME':'AWAY')+'</div></div>';});
  h+='</div><div class="nums6">';
  [[ts.gf_pg,2,'GOALS FOR / GM',lr.gf],[ts.ga_pg,2,'GOALS AGAINST / GM',lr.ga],[ts.pp_pct,1,'POWER PLAY %',lr.pp_pct],[ts.pk_pct,1,'PENALTY KILL %',lr.pk_pct]].forEach(function(x,i){
    if(!has(x[0]))return;
    h+='<div class="nt rise" style="--i:'+(i+1)+'"><div class="nv">'+num(x[0],x[1])+'</div><div class="nl">'+x[2]+'</div>'+(notable('team',x[3])?'<div class="nr">'+ord(x[3]).toUpperCase()+n+'</div>':'')+'</div>';});
  return h+'</div></div>';
}
function storiesHtml(){
  var c=storyCards();if(!c.length)return '';
  return hd('STORYLINES','AROUND THE RAMBLERS')+'<div class="ins">'+c.map(function(x,i){
    var v=x.stat?String(x.stat.value):'',cl=v.length<=2?'':v.length<=4?' m':v.length<=7?' s':' xs';
    return '<div class="ic rise" style="--i:'+i+'"><div class="kd">'+esc(x.kind)+'</div>'+(x.stat?'<div class="sv'+cl+'">'+esc(v)+'</div><div class="sl">'+esc(trunc(String(x.stat.label||'').toUpperCase(),30))+'</div>':'')+'<div class="hl">'+esc(trunc(x.headline,90))+'</div>'+(x.detail?'<div class="dt">'+esc(trunc(x.detail,140))+'</div>':'')+'</div>';}).join('')+'</div>';
}


/* ---------- league scores (Around the MHL) ---------- */
function lgAll(){
  return (B.league_scores||[]).map(function(g){return {g:g,ms:Date.parse(g.start_iso)};}).filter(function(x){return !isNaN(x.ms)&&x.g.home&&x.g.away;}).sort(function(a,b){return a.ms-b.ms;});
}
function lgState(x){return x.ms>now()?'scheduled':x.g.status;} /* a game that has not started yet (time-travel safe) */
function lgLabel(x,st){
  var g=x.g,t=String(g.status_text||'');
  if(st==='scheduled')return {lv:false,t:tnice(x.ms)};
  if(st==='live')return {lv:true,t:(String(g.period||'').toUpperCase()+(has(g.clock)?' '+g.clock:'')).trim()};
  if(st==='intermission')return {lv:true,t:String(g.period||'').toUpperCase()+' INT'};
  if(/SO/i.test(t)||g.ot_so==='SO')return {lv:false,t:'FINAL/SO'};
  if(/OT/i.test(t)||g.ot_so==='OT')return {lv:false,t:'FINAL/OT'};
  return {lv:false,t:'FINAL'};
}
function cityOf(n){var a=String(n||'').split(' ');return a.length>1?a.slice(0,-1).join(' '):a.join(' ');}
function lgToday(){var k=dkey(now());return lgAll().filter(function(x){return dkey(x.ms)===k;});}
function mhlCard(x,i){
  var g=x.g,st=lgState(x),lb=lgLabel(x,st),show=st!=='scheduled'&&has(g.away.goals)&&has(g.home.goals),fin=st==='final';
  function row(t,oth){var w=fin&&+t.goals>+oth.goals;return '<div class="mt'+(w?' win':'')+(lb.lv?' live':'')+'">'+disc(t.logo_url)+'<div class="mn"><b>'+esc(t.abbr)+'</b><small>'+esc(cityOf(t.name))+'</small></div>'+(show?'<div class="sc2">'+esc(t.goals)+'</div>':'<div class="ha2">'+(t===g.home?'HOME':'AWAY')+'</div>')+'</div>';}
  return '<div class="mc rise'+((g.home.is_ramblers||g.away.is_ramblers)?' amh':'')+(lb.lv?' live':'')+'" style="--i:'+i+'"><div class="mst">'+(lb.lv?'<span class="lv">LIVE</span>':'')+'<span>'+esc(lb.t)+'</span></div>'+row(g.away,g.home)+row(g.home,g.away)+'</div>';
}
function mhlNight(list){
  var n=Math.min(6,list.length),one=n<=3;
  return hd('AROUND THE MHL','TONIGHT · '+dnice(now()).toUpperCase())+'<div class="mhl '+(one?'r1':'r2')+(n===4?' c2':'')+'">'+list.slice(0,6).map(mhlCard).join('')+'</div>';
}
function qCard(x){
  var g=x.g,st=lgState(x),lb=lgLabel(x,st),show=st!=='scheduled'&&has(g.away.goals)&&has(g.home.goals),fin=st==='final';
  var aw=fin&&+g.away.goals>+g.home.goals,hw=fin&&+g.home.goals>+g.away.goals;
  var mid=show?(aw?'<b>'+esc(g.away.abbr)+' '+esc(g.away.goals)+'</b>':esc(g.away.abbr)+' '+esc(g.away.goals))+'<em>-</em>'+(hw?'<b>'+esc(g.home.goals)+' '+esc(g.home.abbr)+'</b>':esc(g.home.goals)+' '+esc(g.home.abbr))+(lb.t!=='FINAL'&&fin?'<u>'+esc(lb.t.replace('FINAL/',''))+'</u>':''):esc(g.away.abbr)+'<em>at</em>'+esc(g.home.abbr)+'<u>'+esc(lb.t)+'</u>';
  return '<div class="qc'+((g.home.is_ramblers||g.away.is_ramblers)?' amh':'')+'">'+disc(g.away.logo_url)+'<div class="qt">'+mid+'</div>'+disc(g.home.logo_url)+'</div>';
}
function mhlQuiet(){
  var all=lgAll(),t=now(),today=dkey(t),by={},keys=[];
  all.forEach(function(x){var k=dkey(x.ms);if(k===today)return;if(!by[k]){by[k]=[];keys.push(k);}by[k].push(x);});
  var past=keys.filter(function(k){return dayNum(k)<dayNum(today)&&by[k].some(function(x){return lgState(x)==='final';});}).sort().reverse();
  var fut=keys.filter(function(k){return dayNum(k)>dayNum(today);}).sort();
  var groups=[],rows=0;
  past.slice(0,2).forEach(function(k){var l=by[k].filter(function(x){return lgState(x)==='final';});var r=Math.ceil(l.length/3);if(groups.length&&rows+r>4)return;groups.push({k:k,l:l.slice(0,9),lab:dnice(Date.parse(k+'T15:00:00-03:00')).toUpperCase()});rows+=Math.ceil(Math.min(9,l.length)/3);});
  if(fut.length&&dayNum(fut[0])-dayNum(today)===1){var tl=by[fut[0]].slice(0,6);groups.push({k:fut[0],l:tl,lab:'TOMORROW · '+dnice(Date.parse(fut[0]+'T15:00:00-03:00')).toUpperCase()});}
  if(!groups.length)return '';
  var h='<div class="hd quiet rise"><div class="slab">AROUND THE MHL</div><div class="sub">LATEST SCORES</div></div>';
  groups.forEach(function(gr){h+='<div class="qd rise">'+esc(gr.lab)+'</div><div class="qg rise">'+gr.l.map(qCard).join('')+'</div>';});
  return h;
}
function mhlHtml(){var tn=lgToday();return tn.length?mhlNight(tn):mhlQuiet();}
function mhlHeavy(){return lgToday().length>0;}

/* panel registry: key -> [bug title, builder, dynamic?] */
var REG={
  hero:['GAME DAY',function(m){return m.key==='away'?heroAway(m):heroHome(m);}],
  last:['LAST GAME',lastHtml],
  tape:['TALE OF THE TAPE',function(m){var g=(m.key==='home'||m.key==='away')?m.game:nextUp(now());return tapeHtml(g);}],
  standings:['STANDINGS',standingsHtml],
  leaders:['RAMBLERS LEADERS',leadersHtml],
  streaks:['RUNS & MILESTONES',streaksHtml],
  sched:['COMING UP',schedHtml],
  mhl:['AROUND THE MHL',mhlHtml],
  stories:['STORYLINES',storiesHtml,true]
};
var SEQ={
  home:['hero','tape','mhl','standings','leaders','stories'],
  away:['hero','tape','mhl','standings','leaders','stories'],
  recap:['last','mhl','standings','tape','leaders','stories'],
  off:['standings','mhl','tape','leaders','streaks','sched','stories']
};

/* ---------- ticker: starts at an item boundary, items scroll in from the right ---------- */
function tickerItems(){
  var it=[],us='AMH';
  (B.recent||[]).slice(0,4).forEach(function(g){var o=(g.opp||{}).abbr||'';var a=g.home?us+' '+g.score['for']+'  '+o+' '+g.score.against:o+' '+g.score.against+'  '+us+' '+g.score['for'];it.push(['FINAL'+(g.ot_so?' '+g.ot_so:''),a+'  ('+niceKey(g.date)+')']);});
  var lt=lgToday(),lk=null;
  if(!lt.length){var pk={};lgAll().forEach(function(x){if(lgState(x)==='final'&&x.ms<=now())pk[dkey(x.ms)]=1;});var ks=Object.keys(pk).sort();lk=ks.length?ks[ks.length-1]:null;lt=lk?lgAll().filter(function(x){return dkey(x.ms)===lk&&lgState(x)==='final';}):[];}
  lt.slice(0,6).forEach(function(x){var st=lgState(x),lb=lgLabel(x,st),g=x.g;if(st==='scheduled')return;it.push([lb.lv?'MHL LIVE':'MHL '+lb.t,g.away.abbr+' '+g.away.goals+'  '+g.home.abbr+' '+g.home.goals+(lb.lv?'  ('+lb.t+')':'')]);});
  var nx=nextUp(now());if(nx)it.push(['NEXT',(nx.home?'vs ':'at ')+(nx.opp.name||'')+' - '+dnice(nx.ms)+', '+tnice(nx.ms)]);
  realInsights().slice(0,12).forEach(function(i){it.push([String(i.kind||'INSIGHT').toUpperCase(),trunc(i.headline,110)]);});
  var d=myDiv();if(d&&d.rows[0])it.push(['STANDINGS',d.rows[0].team+' lead '+d.name+' with '+d.rows[0].pts+' points']);
  factItems().forEach(function(f){it.push([f.kind,f.headline]);});
  return it;
}
function buildTicker(force){
  var run=$('tickRun'),win=$('tickWin'),it=tickerItems();
  var one='<span class="cp">'+it.map(function(x){return '<span><b>'+esc(x[0])+'</b>'+esc(x[1])+'</span><em>&#9670;</em>';}).join('')+'</span>';
  var sig=one+(win.clientWidth||0);
  if(!force&&sig===tickSig)return;tickSig=sig;
  run.className='tick-run';run.innerHTML=one;
  var tw=run.firstChild.offsetWidth||3000,W=win.clientWidth||1700,cp=Math.max(2,Math.ceil(W/tw)+1),s='';
  for(var i=0;i<cp;i++)s+=one;
  run.innerHTML=s;
  var sp=120;
  run.style.setProperty('--tw',tw+'px');run.style.setProperty('--w0',W+'px');
  run.style.setProperty('--d0',Math.round((W+tw)/sp)+'s');run.style.setProperty('--d1',Math.round(tw/sp)+'s');
  void run.offsetWidth;run.className='tick-run in';
  run.onanimationend=function(e){if(e.animationName==='tickin'){run.className='tick-run loop';}};
}

/* ---------- bug ---------- */
function buildBug(){
  var ts=B.team_stats||{},ms=srow(B.team.id),rank=ms?ms.r.rank:ts.division_rank;
  $('bugLogo').src=B.team.logo_url||'';
  $('bugRec').textContent=(ts.record||'')+(has(ts.pts)?'  ·  '+ts.pts+' PTS':'')+(notable('div',rank)?'  ·  '+ord(rank).toUpperCase()+' IN '+divWord(B.team.division):'');
  $('credit').textContent=B.copyright||'Official statistics provided by Maritime Hockey League. Powered by HockeyTech.com';
  bugNext();
}
function bugNext(){
  var nx=nextUp(now()),el=$('bugCd');
  if(!nx){$('bugLab').textContent='NEXT GAME';$('bugWhen').textContent='TO BE ANNOUNCED';el.style.display='none';return;}
  var today=dkey(nx.ms)===dkey(now());
  $('bugLab').textContent=today?(nx.home?'HOME GAME':'ROAD GAME'):'NEXT GAME';
  $('bugWhen').textContent=(nx.home?'vs ':'at ')+(nx.opp.abbr||'')+'  '+(today?'TODAY':dnice(nx.ms))+', '+tnice(nx.ms);
  el.style.display='';el.dataset.ms=nx.ms;
}
function p2(x){return (x<10?'0':'')+x;}
function tick(){
  if(!B)return;
  var t=now(),el=$('bugCd'),ms=+el.dataset.ms;
  if(ms){var d=ms-t;if(d<=0)el.textContent='GAME ON';else{var s=Math.floor(d/1000),dd=Math.floor(s/86400);el.textContent=(dd?dd+'D ':'')+p2(Math.floor(s%86400/3600))+':'+p2(Math.floor(s%3600/60))+':'+p2(s%60);}}
  for(var i=0;i<cdEls.length;i++){
    var c=cdEls[i],r=+c.dataset.ms-t;
    if(r>0){var x=Math.floor(r/1000);c.textContent=p2(Math.floor(x/3600))+':'+p2(Math.floor(x%3600/60))+':'+p2(x%60);}
    else c.textContent=r>-3*3600e3?'GAME ON':'FINAL SOON';
  }
}

/* ---------- stage ---------- */
function ensureStage(){
  if(panels.length)return;
  var st=$('stage'),ds=$('dots');
  for(var i=0;i<MAXP;i++){var p=document.createElement('section');p.className='panel';st.appendChild(p);panels.push(p);var d=document.createElement('i');ds.appendChild(d);dots.push(d);}
}
function fillAll(){
  mode=pickMode();seq=[];
  var keys=SEQ[mode.key];
  keys.forEach(function(k){var h='';try{h=REG[k][1](mode)||'';}catch(e){h='';if(window.console)console.error(e);}if(h){var i=seq.length;panels[i].innerHTML=h;panels[i].dataset.key=k;panels[i].dataset.q=(k==='mhl'&&!mhlHeavy())?'1':'';seq.push(k);}});
  for(var j=seq.length;j<MAXP;j++){panels[j].innerHTML='';panels[j].className='panel';delete panels[j].dataset.key;}
  for(var q=0;q<MAXP;q++)dots[q].style.display=q<seq.length?'':'none';
  cdEls=[].slice.call(document.querySelectorAll('.cd'));
}
function show(i){
  var prev=cur>=0&&panels[cur]?panels[cur]:null;
  if(seq[i]==='stories'&&REG.stories[2]){panels[i].innerHTML=storiesHtml();storyPage++;}
  $('bugSec').textContent=seq[i]==='hero'&&mode?mode.label:REG[seq[i]][0];
  for(var k=0;k<MAXP;k++)dots[k].className=k===i?'on':'';
  if(prev&&prev!==panels[i]){prev.className='panel out';setTimeout(function(){if(prev!==panels[cur])prev.className='panel';},100);}
  panels[i].className='panel active';
  cur=i;
  var pr=$('prog');pr.className='';void pr.offsetWidth;pr.className='run';
  cdEls=[].slice.call(document.querySelectorAll('.cd'));tick();
}
function next(){
  clearTimeout(timer);
  if(!seq.length)return;
  var n=(cur+1)%seq.length;
  if(cur>=0&&n===0)cycle++;
  if(panels[n].dataset.q&&cycle%2===1&&seq.length>1)n=(n+1)%seq.length; /* quiet league page: every other cycle */
  if(cur<0||seq.length===1){show(n);}
  else{
    var w=$('wipe');w.className='';void w.offsetWidth;w.className='go';
    setTimeout(function(){show(n);},480);
    setTimeout(function(){w.className='';},1100);
  }
  timer=setTimeout(next,DWELL);
}
function sigNow(){mode=mode||pickMode();var m=pickMode();return m.key+'|'+(m.game?m.game.id:'')+'|'+(B&&B.generated_at)+'|'+(INS&&INS.generated_at);}
function render(){
  var sig=sigNow();
  if(sig===lastSig){bugNext();return;}
  lastSig=sig;
  fillAll();buildBug();buildTicker(true);
  cur=-1;clearTimeout(timer);next();
}
function load(){
  return Promise.all([fetchJson('../../data/board.json'),fetchJson('../../data/insights.json')]).then(function(r){
    if(r[0])B=r[0];if(r[1])INS=r[1];
    if(!B||!B.team)return;
    if(fixture)return fetchJson('./'+fixture).then(function(f){if(f&&f.league_scores)B.league_scores=f.league_scores;go();});
    go();
  });
}
function go(){
  {
    document.documentElement.style.setProperty('--dwell',DWELL+'ms');
    ensureStage();render();
  }
}
load();
setInterval(load,REFRESH);
setInterval(function(){if(B)tick();},1000);
setInterval(function(){if(B)render();},30000); /* calendar roll-over: re-picks mode when midnight passes or a game ends */
if(document.fonts&&document.fonts.ready)document.fonts.ready.then(function(){if(B)buildTicker(true);});
})();
