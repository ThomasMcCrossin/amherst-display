(function(){
'use strict';
var TZ='America/Halifax',FRAME_MS=12000,REFRESH_MS=300000,NPANELS=4;
var qs=new URLSearchParams(location.search),override=qs.get('now'),offset=0;
if(override){var t=Date.parse(override);if(!isNaN(t))offset=t-Date.now();else override=null;}
function now(){return Date.now()+offset;}
var B=null,I=null,mode=null,frame=0,cdEls=[],panels=[],dots=[],lastSig='';
function $(id){return document.getElementById(id);}
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});}
function has(v){return v!=null&&v!==''&&!(typeof v==='number'&&isNaN(v));}
function num(v,d){return has(v)&&isFinite(+v)?(+v).toFixed(d):'';}
function pct3(v){return has(v)?(+v).toFixed(3).replace(/^0/,''):'';}
var dfDate=new Intl.DateTimeFormat('en-CA',{timeZone:TZ,year:'numeric',month:'2-digit',day:'2-digit'});
var dfParts=new Intl.DateTimeFormat('en-US',{timeZone:TZ,weekday:'short',month:'short',day:'numeric'});
var dfTime=new Intl.DateTimeFormat('en-US',{timeZone:TZ,hour:'numeric',minute:'2-digit'});
var dfHour=new Intl.DateTimeFormat('en-US',{timeZone:TZ,hour:'numeric',hour12:false});
function dkey(ms){return dfDate.format(new Date(ms));}
function dnice(ms){var p={};dfParts.formatToParts(new Date(ms)).forEach(function(x){p[x.type]=x.value;});return p.weekday+' '+p.month+' '+p.day;}
function tnice(ms){return dfTime.format(new Date(ms));}
function dayNum(k){var a=k.split('-');return Math.round(Date.UTC(+a[0],+a[1]-1,+a[2])/864e5);}
function niceKey(k){return dnice(Date.parse(k+'T15:00:00-03:00'));}
function logo(u,cls){return u?'<span class="disc '+(cls||'')+'"><img src="'+esc(u)+'" alt=""></span>':'';}
function shot(id,url){var u=url||(id?'https://assets.leaguestat.com/mhl/240x240/'+id+'.jpg':'');return u?'<img src="'+esc(u)+'" alt="" onerror="this.style.visibility=\'hidden\'">':'';}
function rc(r){return r&&r.overall?r.overall:'';}

/* ---------- schedule + mode ---------- */
function games(){
  var out=[];
  (B.recent||[]).forEach(function(g){out.push({id:g.id,ms:Date.parse(g.start_iso),home:g.home,opp:g.opp,venue:g.venue,final:true,rec:g});});
  (B.upcoming||[]).forEach(function(g){out.push({id:g.id,ms:Date.parse(g.start_iso),home:g.home,opp:g.opp,venue:g.venue,final:false});});
  out.sort(function(a,b){return a.ms-b.ms;});return out;
}
function pickMode(){
  var t=now(),today=dkey(t),G=games(),i,g,td=null,yd=null,ydn=dayNum(today)-1;
  for(i=0;i<G.length;i++){g=G[i];var k=dkey(g.ms);if(k===today)td=g;if(g.final&&dayNum(k)===ydn)yd=g;}
  if(td&&td.final)return {key:'recap',label:'Final score',game:td,when:'tonight'};
  if(td)return {key:td.home?'home':'away',label:td.home?'Home game day':'Away game day',game:td};
  if(yd)return {key:'recap',label:'Day-after recap',game:yd,when:'last night'};
  return {key:'off',label:'Off day'};
}
function nextUp(after){var G=games();for(var i=0;i<G.length;i++)if(!G[i].final&&G[i].ms>after)return G[i];return null;}
function fullNG(g){var n=B.next_game;return n&&g&&n.id===g.id?n:null;}
function srow(id){var D=B.standings&&B.standings.divisions||[];for(var i=0;i<D.length;i++)for(var j=0;j<D[i].rows.length;j++)if(D[i].rows[j].team_id===id)return D[i].rows[j];return null;}
function myDiv(){var D=B.standings.divisions;for(var i=0;i<D.length;i++)for(var j=0;j<D[i].rows.length;j++)if(D[i].rows[j].is_ramblers)return D[i];return D[0];}
function ordinal(n){var s=['th','st','nd','rd'],v=n%100;return n+(s[(v-20)%10]||s[v]||s[0]);}
function resChip(r){return '<span class="chip '+r+'">'+r+'</span>';}
function lastFive(){return (B.recent||[]).slice(0,5);}

/* ---------- shared pieces ---------- */
function head(a,b,sub){return '<h2>'+a+(b?' <span>'+b+'</span>':'')+'</h2>'+(sub?'<div class="sub">'+sub+'</div>':'');}
function tape(g){
  var opp=g.opp,os=srow(opp.id),ms=srow(B.team.id);
  if(!os||!ms)return '<div class="empty">Matchup numbers coming soon</div>';
  var gpo=os.gp||0,gpm=ms.gp||0;
  var rows=[
    ['Record',os.w+'-'+os.l+'-'+(os.otl+os.sol),ms.w+'-'+ms.l+'-'+(ms.otl+ms.sol),0],
    ['Points %',pct3(os.pct),pct3(ms.pct),+ms.pct-+os.pct],
    ['Goals / game',gpo?num(os.gf/gpo,2):'',gpm?num(ms.gf/gpm,2):'',gpm&&gpo?ms.gf/gpm-os.gf/gpo:0],
    ['Against / game',gpo?num(os.ga/gpo,2):'',gpm?num(ms.ga/gpm,2):'',gpm&&gpo?os.ga/gpo-ms.ga/gpm:0],
    ['Power play',has(os.pp_pct)?num(os.pp_pct,1)+'%':'',has(ms.pp_pct)?num(ms.pp_pct,1)+'%':'',+ms.pp_pct-+os.pp_pct],
    ['Penalty kill',has(os.pk_pct)?num(os.pk_pct,1)+'%':'',has(ms.pk_pct)?num(ms.pk_pct,1)+'%':'',+ms.pk_pct-+os.pk_pct]
  ];
  var h='<div class="thead"><div class="t">'+logo(B.team.logo_url)+'AMH</div><div class="t">'+esc(opp.abbr)+logo(opp.logo_url)+'</div></div><div class="tape">';
  rows.forEach(function(r){
    if(!r[1]&&!r[2])return;
    var m=r[3]>0.0001,o=r[3]<-0.0001;
    h+='<div class="v'+(m?' b':'')+'">'+esc(r[2])+'</div><div class="l">'+r[0]+'</div><div class="v'+(o?' b':'')+'">'+esc(r[1])+'</div>';
  });
  return h+'</div>';
}
function h2hPanel(g){
  var n=fullNG(g),opp=g.opp,th=[],ls=[];
  (B.recent||[]).forEach(function(r){if(r.opp.id===opp.id)th.push({date:r.date,home:r.home,s:r.score,res:r.result,ot:r.ot_so});});
  if(n){th=n.h2h_this_season.map(function(r){return {date:r.date,home:r.home,s:r.score,res:r.result,ot:r.ot_so};});ls=n.h2h_last_season.map(function(r){return {date:r.date,home:r.home,s:r.score,res:r.result,ot:r.ot_so};});}
  function lines(a,empty){if(!a.length)return '<div class="line mute">'+empty+'</div>';return a.slice(0,3).map(function(r){return '<div class="line">'+resChip(r.res)+'<div class="grow">'+niceKey(r.date).replace(/^\w+ /,'')+' <span class="mute">'+(r.home?'home':'away')+'</span></div><div class="sc">'+r.s['for']+'-'+r.s.against+(r.ot?' <small class="mute" style="font-size:30px">'+r.ot+'</small>':'')+'</div></div>';}).join('');}
  var left='<div class="card col"><h3>Head to head</h3><div class="mute" style="font-size:30px;margin-bottom:4px">THIS SEASON</div>'+lines(th,'First meeting of 2026-27')+(n||ls.length?'<div class="mute" style="font-size:30px;margin:14px 0 4px">LAST SEASON</div>'+lines(ls,'No games last season'):'');
  if(n&&n.h2h_records&&n.h2h_records.last_5_years){var r5=n.h2h_records.last_5_years,a=r5.home_team,b=r5.visiting_team;left+='<div class="mute" style="font-size:30px;margin:14px 0 0">LAST 5 YEARS: Amherst '+a.w+'-'+a.l+(a.otl+a.sl?'-'+(a.otl+a.sl):'')+', '+esc(opp.abbr)+' '+b.w+'-'+b.l+(b.otl+b.sl?'-'+(b.otl+b.sl):'')+'</div>';}
  left+='</div>';
  var os=srow(opp.id),form='';
  if(n&&n.opponent_form&&n.opponent_form.last5.length){form=n.opponent_form.last5.slice(0,4).map(function(f){return '<div class="line">'+resChip(f.result)+'<div class="grow">'+(f.home?'vs':'at')+' '+esc(f.opp)+' <span class="mute">'+niceKey(f.date).replace(/^\w+ /,'')+'</span></div><div class="sc">'+f.gf+'-'+f.ga+'</div></div>';}).join('');}
  else if(os){form='<div class="line"><div class="grow">Last 10</div><div class="sc gold">'+esc(os.last10)+'</div></div><div class="line"><div class="grow">Streak (W-L-OTL-SOL)</div><div class="sc gold">'+esc(os.streak)+'</div></div>';}
  var mine=lastFive().slice(0,4).map(function(r){return '<div class="line">'+resChip(r.result)+'<div class="grow">'+(r.home?'vs':'at')+' '+esc(r.opp.abbr)+' <span class="mute">'+niceKey(r.date).replace(/^\w+ /,'')+'</span></div><div class="sc">'+r.score['for']+'-'+r.score.against+'</div></div>';}).join('');
  var right='<div class="card col"><h3>'+esc(opp.abbr)+' recent form</h3>'+form+'</div><div class="card col"><h3>Ramblers recent form</h3>'+mine+'</div>';
  return head('Form','guide','Where both teams are coming in')+'<div class="cols">'+left+right.replace(/^/,'')+'</div>';
}
function pl(p,extra){return '<div class="pl"><div class="hs">'+shot(p.id,p.headshot_url)+'</div><div class="nmb">'+esc(p.name)+'<small>'+(has(p.number)?'#'+p.number:'')+(p.pos?' '+esc(p.pos):'')+(extra?' '+extra:'')+'</small></div><div class="st">'+p.pts+'<small>'+p.g+'G '+p.a+'A</small></div></div>';}
function oppNums(opp){var r=srow(opp.id);if(!r)return '<div class="empty">Scouting report coming soon</div>';var gp=r.gp||1;
  function ln(a,b){return '<div class="line"><div class="grow">'+a+'</div><div class="sc gold">'+esc(b)+'</div></div>';}
  return ln('Record',r.w+'-'+r.l+'-'+(r.otl+r.sol))+ln('Goals per game',num(r.gf/gp,2))+ln('Power play',has(r.pp_pct)?num(r.pp_pct,1)+'%':'')+ln('Last 10',r.last10||'');}
function watchPanel(g){
  var n=fullNG(g),opp=g.opp,oppl=[],ours,gl='';
  if(n&&n.opponent.leading_scorers&&n.opponent.leading_scorers.length)oppl=n.opponent.leading_scorers.slice(0,3);
  else oppl=((B.league_leaders&&B.league_leaders.points)||[]).filter(function(p){return p.team===opp.abbr;}).slice(0,3);
  ours=(B.skaters||[]).slice(0,3).map(function(s){var l5=0;(s.last5||[]).forEach(function(x){l5+=x.pts||0;});return pl(s,(s.last5&&s.last5.length)?'':'');}).join('');
  var o=oppl.length?oppl.map(function(p){return pl(p);}).join(''):oppNums(opp);
  var st=n&&n.official_starters||{home:null,away:null};
  function gname(x,ab){return '<div>'+ab+': <b>'+(x&&x.name?esc(x.name):'Starter TBA')+'</b></div>';}
  var homeAb=g.home?'AMH':opp.abbr,awayAb=g.home?opp.abbr:'AMH';
  gl='<div class="card gl"><span class="mute" style="font-weight:800;letter-spacing:3px">STARTING GOALIES</span>'+gname(st.home,homeAb)+gname(st.away,awayAb)+'<span class="mute" style="margin-left:auto;font-size:28px">Posted when the official lineup is set</span></div>';
  return head('Players','to watch',g.home?'Tonight at the Amherst Stadium':'On the road tonight')+'<div class="cols" style="margin-top:14px"><div class="card col" style="padding:14px 30px"><h3 style="margin-bottom:0">'+esc(opp.name)+'</h3>'+o+'</div><div class="card col" style="padding:14px 30px"><h3 style="margin-bottom:0">Ramblers</h3>'+ours+'</div></div>'+gl;
}
function schedPanel(skipId){
  var U=(B.upcoming||[]).filter(function(g){return g.id!==skipId;}).slice(0,6);
  return head('Coming','up','The next games on the Ramblers schedule')+'<div style="margin-top:18px">'+U.map(function(g){var ms=Date.parse(g.start_iso);return '<div class="card sched"><div class="d">'+dnice(ms)+'<small>'+tnice(ms)+'</small></div><div class="ha '+(g.home?'h':'')+'">'+(g.home?'HOME':'AWAY')+'</div>'+logo(g.opp.logo_url)+'<div>'+esc(g.opp.name)+'</div><div class="vn">'+esc(g.venue||'')+'</div></div>';}).join('')+'</div>';
}
function standTable(div,focusAbbr,hl){
  var rows=div.rows.slice(0,7);
  return '<table><thead><tr><th>TEAM</th><th>GP</th><th>W</th><th>L</th><th>OT</th><th>PTS</th><th>DIFF</th><th>LAST 10</th></tr></thead><tbody>'+rows.map(function(r){var d=r.diff>0?'+'+r.diff:r.diff;return '<tr class="'+(r.is_ramblers?'me':'')+'"><td class="tm">'+(r.rank)+'&nbsp;&nbsp;'+logo(r.logo_url)+esc(r.abbr)+'</td><td>'+r.gp+'</td><td>'+r.w+'</td><td>'+r.l+'</td><td>'+(r.otl+r.sol)+'</td><td class="pts">'+r.pts+'</td><td>'+d+'</td><td>'+esc(r.last10||'')+'</td></tr>';}).join('')+'</tbody></table>';
}

/* ---------- mode panel builders ---------- */
function cdBlock(g,small){
  return '<div class="big'+(small?' sm':'')+' cd" data-ms="'+g.ms+'"></div>';
}
function tonight(g){var h=+dfHour.format(new Date(g.ms));return dkey(now())===dkey(g.ms)?(h>=17?'Tonight':'Today'):dnice(g.ms);}
function panelsHome(m){
  var g=m.game,opp=g.opp,ms=srow(B.team.id),os=srow(opp.id),n=fullNG(g);
  var rcm=ms?ms.w+'-'+ms.l+'-'+(ms.otl+ms.sol):rc(B.team_stats&&{overall:B.team_stats.record}),rco=os?os.w+'-'+os.l+'-'+(os.otl+os.sol):'';
  var p1='<div class="hero"><div class="team">'+logo(B.team.logo_url)+'<div class="nm">Ramblers</div><div class="rc">'+esc(rcm)+'</div></div><div class="mid"><div class="lbl gold">'+tonight(g)+' at '+esc(g.venue||'the rink')+'</div>'+cdBlock(g)+'<div class="lbl">until puck drop</div><div class="pill">'+tnice(g.ms)+' &middot; '+dnice(g.ms)+'</div></div><div class="team">'+logo(opp.logo_url)+'<div class="nm">'+esc(opp.name).replace(/ (?=[^ ]+$)/,'<br>')+'</div><div class="rc">'+esc(rco)+'</div></div></div>';
  return [p1,head('Tale of','the tape','Ramblers vs '+esc(opp.name))+tape(g),h2hPanel(g),watchPanel(g)];
}
function panelsAway(m){
  var g=m.game,opp=g.opp,os=srow(opp.id),n=fullNG(g);
  var p1='<div class="hero"><div class="team">'+logo(opp.logo_url)+'<div class="nm">@ '+esc(opp.abbr)+'</div><div class="rc">'+esc(opp.name)+'</div></div><div class="mid"><div class="lbl gold">'+tonight(g)+' on the road</div>'+cdBlock(g,true)+'<div class="lbl">until puck drop &middot; '+tnice(g.ms)+'</div><div class="card" style="margin:30px auto 0;max-width:860px;padding:22px 30px;border-color:var(--gold)"><div class="an gold" style="font-size:78px;line-height:1">WATCH LIVE ON FLOHOCKEY</div><div style="font-size:36px;margin-top:10px">'+(n&&n.flo_url?'Search Amherst Ramblers on flohockey.tv':'Find the Ramblers game on flohockey.tv')+'</div><div class="mute" style="font-size:32px;margin-top:6px">'+esc(g.venue||'')+'</div></div></div></div>';
  return [p1,head('Tale of','the tape','Ramblers vs '+esc(opp.name))+tape(g),h2hPanel(g),watchPanel(g)];
}
function panelsRecap(m){
  var r=m.game.rec,opp=r.opp,sc=r.score,res=r.result,win=res==='W';
  var verdict=win?'WIN':(res==='L'?'LOSS':res==='OTL'?'OT LOSS':'SHOOTOUT LOSS');
  var goals=(r.scorers||[]).slice(0,5).map(function(x){return '<div class="goal"><span class="t">'+esc(x.period)+' '+esc(x.time)+'</span><span>'+esc(x.scorer)+(x.pp?' (PP)':'')+(x.assists&&x.assists.length?' <i>'+esc(x.assists.join(', '))+'</i>':'')+'</span></div>';}).join('')||'<div class="goal mute">No Ramblers goals</div>';
  var p1=head('Final','score',niceKey(r.date)+(r.ot_so?' &middot; '+r.ot_so:'')+(has(r.attendance)?' &middot; '+r.attendance+' fans':''))+'<div class="final">'+logo(B.team.logo_url)+'<div class="sc '+(win?'gold':'')+'">'+sc['for']+'</div><div class="dash">-</div><div class="sc">'+sc.against+'</div>'+logo(opp.logo_url)+'</div><div style="text-align:center;margin-top:0"><span class="verdict '+res+'">'+verdict+'</span><span class="sub" style="margin-left:24px">'+(r.home?'vs':'at')+' '+esc(opp.name)+'</span></div><div class="card" style="margin-top:22px;padding:18px 32px"><div class="mute" style="font-size:28px;letter-spacing:3px;font-weight:800">RAMBLERS GOALS</div>'+goals+'</div>';
  var stars=(r.three_stars||[]).slice(0,3).map(function(s){return '<div class="card star"><div class="hs">'+shot(s.id)+'</div><div class="n">'+'&#9733;'.repeat(0)+s.star+'</div><div class="p">'+esc(s.name)+'</div><div class="ln">'+esc(s.team)+(has(s.number)?' #'+s.number:'')+' &middot; '+esc(s.line||'')+'</div></div>';}).join('');
  var sh=r.shots||{},st='';
  function cell(v,l){return has(v)&&v!==''?'<div class="card"><b>'+esc(v)+'</b>'+l+'</div>':'';}
  st=cell(has(sh['for'])?sh['for']+' - '+sh.against:'','Shots')+cell(r.pp&&r.pp['for']?r.pp['for']+' / '+r.pp.against:'','Power play (G/opps)')+cell(r.pim?r.pim['for']+' - '+r.pim.against:'','Penalty minutes');
  var p2=head('Three','stars',(r.home?'vs ':'at ')+esc(opp.name))+'<div class="stars">'+stars+'</div><div class="strip">'+st+'</div>';
  var d=myDiv(),me=d.rows.filter(function(x){return x.is_ramblers;})[0],txt='';
  if(me){var r3=d.rows[2],r4=d.rows[3];txt=me.rank>3&&r3?'Ramblers sit '+ordinal(me.rank)+' with '+me.pts+' points, '+(r3.pts-me.pts)+' back of '+ordinal(3)+' place':'Ramblers sit '+ordinal(me.rank)+' with '+me.pts+' points';}
  var p3=head('Standings','shake-up',esc(d.name)+' division')+(txt?'<div class="note" style="color:var(--gold);font-weight:800;font-size:38px">'+txt+'</div>':'')+standTable(d);
  var nx=nextUp(m.game.ms+1),p4;
  if(nx){var days=dayNum(dkey(nx.ms))-dayNum(dkey(now()));
    p4=head('Next','up',days<=0?'Today':days===1?'Tomorrow':'In '+days+' days')+'<div class="hero" style="height:620px"><div class="team">'+logo(nx.opp.logo_url)+'<div class="nm">'+esc(nx.opp.name)+'</div></div><div class="mid"><div class="lbl gold">'+(nx.home?'Home':'Away')+'</div><div class="big sm" style="font-size:130px">'+dnice(nx.ms)+'</div><div class="pill">'+tnice(nx.ms)+' &middot; '+esc(nx.venue||'')+'</div></div></div>';
  }else p4=head('Last','five','')+'<div class="empty">Schedule update coming</div>';
  var lf='<div class="card" style="padding:14px 30px;margin-top:-14px"><div class="row" style="gap:20px;font-size:34px"><span class="mute" style="font-weight:800;letter-spacing:3px">LAST 5</span>'+lastFive().map(function(x){return resChip(x.result)+'<span>'+x.score['for']+'-'+x.score.against+'</span>';}).join('')+'</div></div>';
  return [p1,p2,p3,p4+lf];
}
function panelsOff(m){
  var d=myDiv();
  var p1=head('Division','standings',esc(d.name)+' &middot; '+(B.season||''))+standTable(d);
  var rr=(B.league_leaders&&B.league_leaders.ramblers_ranks&&B.league_leaders.ramblers_ranks.points)||[];
  var rank={};rr.forEach(function(x){rank[x.id]=x.rank;});
  var lead=(B.skaters||[]).slice(0,5).map(function(s){return '<div class="pl" style="padding:10px 0"><div class="hs" style="width:100px;height:100px">'+shot(s.id,s.headshot_url)+'</div><div class="nmb">'+esc(s.name)+'<small>#'+s.number+' '+esc(s.pos||'')+(rank[s.id]?' &middot; '+ordinal(rank[s.id])+' in the MHL':'')+'</small></div><div class="st">'+s.pts+'<small>'+s.g+'G '+s.a+'A</small></div></div>';}).join('');
  var p2=head('Ramblers','leaders','Points, 2026-27')+'<div class="card" style="padding:10px 34px;margin-top:16px">'+lead+'</div>';
  var sk=((B.streaks&&B.streaks.ramblers)||[]).slice(0,4).map(function(s){var k=s.kind==='goals'?'goal streak':s.kind==='points'?'point streak':s.kind+' streak';return '<div class="line"><div class="grow">'+esc(s.name)+'<br><span class="mute" style="font-size:30px">'+s.games+'-game '+k+(s.active?' (active)':'')+'</span></div><div class="sc gold">'+(s.kind==='goals'?s.g+'G':s.pts+'P')+'</div></div>';}).join('')||'<div class="line mute">No streaks to report</div>';
  var ms=(B.milestones_near||[]).slice(0,5).map(function(x){return '<div class="line"><div class="grow">'+esc(x.name)+'<br><span class="mute" style="font-size:30px">'+x.needs+' '+(x.needs===1?'away from':'to')+' '+x.milestone+' '+esc(x.stat)+'</span></div><div class="sc gold">'+x.current+'</div></div>';}).join('')||'<div class="line mute">No milestones this close</div>';
  var p3=head('Streaks','&amp; milestones','Who is hot, who is close')+'<div class="cols"><div class="card col"><h3>Runs this season</h3>'+sk+'</div><div class="card col"><h3>Milestones within reach</h3>'+ms+'</div></div>';
  return [p1,p2,p3,schedPanel()];
}
var BUILD={home:panelsHome,away:panelsAway,recap:panelsRecap,off:panelsOff};

/* ---------- render / loop ---------- */
function insights(){if(!I||I.stub||!I.items)return [];return I.items.filter(function(x){return x&&x.headline;}).sort(function(a,b){return (a.priority||9)-(b.priority||9);}).slice(0,6);}
function render(){
  var m=pickMode();mode=m;
  var sig=m.key+'|'+(m.game?m.game.id:'')+'|'+(B&&B.generated_at)+'|'+(I&&I.generated_at);
  $('mode').firstChild.nextSibling.nodeValue=m.label+(override?' (preview)':'');
  if(sig===lastSig)return;lastSig=sig;
  var arr;try{arr=BUILD[m.key](m);}catch(e){arr=['<div class="empty">Board data updating</div>'];if(window.console)console.error(e);}
  for(var i=0;i<NPANELS;i++)panels[i].innerHTML=arr[i]||'';
  cdEls=[].slice.call(document.querySelectorAll('.cd'));
  tick();
}
function tick(){
  var t=now();
  $('clock').innerHTML=tnice(t)+'<b>'+dnice(t)+'</b>';
  cdEls.forEach(function(el){
    var d=+el.dataset.ms-t,s;
    if(d>0){var x=Math.floor(d/1000),h=Math.floor(x/3600),mi=Math.floor(x%3600/60);s=Math.floor(x%60);el.textContent=(h<10?'0':'')+h+':'+(mi<10?'0':'')+mi+':'+(s<10?'0':'')+s;}
    else if(d>-3*3600e3)el.textContent='GAME ON';
    else el.textContent='FINAL SOON';
  });
}
function setFrame(i){
  frame=i%NPANELS;
  for(var k=0;k<NPANELS;k++){panels[k].classList.toggle('on',k===frame);dots[k].classList.toggle('on',k===frame);}
  var it=insights(),ie=$('ins');
  if(it.length){var x=it[frame%it.length];ie.innerHTML='<b>Did you know</b>'+esc(x.headline)+(x.stat&&has(x.stat.value)?' &middot; '+esc(x.stat.value)+' '+esc(x.stat.label||''):'');}
  else ie.innerHTML='';
}
function load(){
  var ts='?t='+Date.now();
  Promise.all([
    fetch('../../data/board.json'+ts,{cache:'no-store'}).then(function(r){return r.json();}),
    fetch('../../data/insights.json'+ts,{cache:'no-store'}).then(function(r){return r.ok?r.json():null;}).catch(function(){return null;})
  ]).then(function(a){B=a[0];I=a[1];$('cr').textContent=B.copyright||'';render();setFrame(frame);}).catch(function(e){if(window.console)console.error(e);});
}
function init(){
  var a=$('area'),d=$('dots');
  for(var i=0;i<NPANELS;i++){var p=document.createElement('div');p.className='panel';a.appendChild(p);panels.push(p);var q=document.createElement('i');d.appendChild(q);dots.push(q);}
  setInterval(tick,1000);
  setInterval(function(){if(B){render();}setFrame(frame+1);},FRAME_MS);
  setInterval(load,REFRESH_MS);
  load();
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
