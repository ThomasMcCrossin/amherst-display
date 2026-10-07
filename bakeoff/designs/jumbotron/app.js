(function(){
'use strict';
var FRAME_MS=10000, TZ='America/Halifax';
var B=null, INS=null, panels=[], idx=-1, startMs=0, fillBtn=null;
var $=function(id){return document.getElementById(id)};
function has(v){return v!==null&&v!==undefined&&v!==''&&!(typeof v==='number'&&isNaN(v))}
function set(id,t){var e=$(id);if(e)e.textContent=has(t)?String(t):''}
function img(id,u){var e=$(id);if(!e)return;if(has(u)){if(e.getAttribute('src')!==u)e.src=u;e.style.display=''}else{e.removeAttribute('src');e.style.display='none'}}
function parts(d,o){var m={};new Intl.DateTimeFormat('en-US',Object.assign({timeZone:TZ},o)).formatToParts(d).forEach(function(p){m[p.type]=p.value});return m}
function dateStr(d){var p=parts(d,{weekday:'short',month:'short',day:'numeric'});return p.weekday+' '+p.month+' '+p.day}
function timeStr(d){var p=parts(d,{hour:'numeric',minute:'2-digit',hour12:true});return p.hour+':'+p.minute+' '+p.dayPeriod.toUpperCase()}
function ord(n){var s=['TH','ST','ND','RD'],v=n%100;return n+(s[(v-20)%10]||s[v]||s[0])}
function dayLabel(d){var a=parts(d,{year:'numeric',month:'numeric',day:'numeric'}),b=parts(new Date(),{year:'numeric',month:'numeric',day:'numeric'});
 var da=Date.UTC(a.year,a.month-1,a.day),db=Date.UTC(b.year,b.month-1,b.day),n=Math.round((da-db)/864e5);
 return n===0?'TONIGHT':n===1?'TOMORROW':dateStr(d).toUpperCase()}
function j(u){return fetch(u,{cache:'no-store'}).then(function(r){if(!r.ok)throw new Error(u);return r.json()}).catch(function(){return null})}

function recW(r){return r&&r.overall?r.overall.split('-').join('-'):null}

/* ---------- panel fillers; each returns true if it has data ---------- */
function fillNext(){
 var g=B.next_game;if(!g||!g.opponent)return false;
 var o=g.opponent,d=new Date(g.start_iso);
 set('p1tag',g.home?'Next game · Home':'Next game · Away');
 img('p1l',B.team.logo_url);img('p1r',o.logo_url);
  set('p1when',dayLabel(d)==='TONIGHT'||dayLabel(d)==='TOMORROW'?dayLabel(d)+' '+timeStr(d):dateStr(d).toUpperCase()+' · '+timeStr(d));
 var s=[];if(has(g.venue))s.push(g.venue.toUpperCase());
 var h=g.official_starters&&(g.official_starters.home||g.official_starters.away);
 s.push(h?'STARTER: '+h.name.toUpperCase():'STARTER TBA');
 set('p1sub',s.join('  ·  '));
 return true}
function tickCd(){
 if(!B||!B.next_game){set('p1cd','');return}
 var ms=new Date(B.next_game.start_iso)-Date.now();
 if(isNaN(ms)){set('p1cd','');return}
 if(ms<=0){set('p1cd',ms>-3*3600e3?'PUCK DROP':'');return}
 var s=Math.floor(ms/1000),dd=Math.floor(s/86400),hh=Math.floor(s%86400/3600),mm=Math.floor(s%3600/60),ss=s%60;
 function z(n){return(n<10?'0':'')+n}
 set('p1cd',(dd>0?dd+'D ':'')+z(hh)+'H '+z(mm)+'M '+(dd>0?'':z(ss)+'S'))}
function fillLast(){
 var r=B.recent&&B.recent[0];if(!r||!r.score)return false;
 var f=r.score['for'],a=r.score.against;if(!has(f)||!has(a))return false;
 set('p2tag','Last game · Final'+(r.ot_so?' / '+r.ot_so:''));
 img('p2l',B.team.logo_url);img('p2r',r.opp&&r.opp.logo_url);set('p2on',r.opp&&r.opp.abbr);
 var sc=$('p2sc');sc.textContent='';var x=document.createTextNode(f+' ');var i=document.createElement('i');i.textContent='-';sc.appendChild(x);sc.appendChild(i);sc.appendChild(document.createTextNode(' '+a));
 var w=r.result==='W',res=$('p2res');
 res.textContent=w?'RAMBLERS WIN':(r.ot_so==='SO'?'LOSS IN THE SHOOTOUT':r.ot_so==='OT'?'LOSS IN OVERTIME':'RAMBLERS FALL');
 res.style.color=w?'var(--gold)':'var(--red)';
 var sc2=r.scorers&&r.scorers.length?r.scorers.slice(0,3).map(function(g){return g.scorer.split(' ').slice(-1)[0].toUpperCase()}).join('  ·  '):'';
 var bits=[];if(sc2)bits.push('GOALS: '+sc2);
 if(r.shots&&has(r.shots['for'])&&has(r.shots.against))bits.push('SHOTS '+r.shots['for']+'-'+r.shots.against);
 set('p2scr',bits.join('   ·   '));
 return true}
function fillStar(){
 var s=B.skaters&&B.skaters[0];if(!s||!has(s.pts))return false;
 img('p3img',s.headshot_url);set('p3no',has(s.number)?'#'+s.number+(s.pos?'  '+s.pos:''):s.pos);
 set('p3nm',s.name);set('p3pts',s.pts);
 var bits=[s.g+' G',s.a+' A',s.gp+' GP'];
 var rk=B.league_leaders&&B.league_leaders.ramblers_ranks&&B.league_leaders.ramblers_ranks.points;
 var me=rk&&rk.filter(function(x){return x.id===s.id})[0];
 set('p3ga',bits.join('  ·  ')+(me?'  ·  '+ord(me.rank)+' IN MHL':''));
 return true}
var rows=[];
function buildRows(){var p=$('p4');for(var i=0;i<6;i++){var r=document.createElement('div');r.className='row big';r.style.top=(150+i*128)+'px';
 r.innerHTML='<span class="rk"></span><span class="disc"><img alt=""></span><span class="ab"></span><span class="gp"></span><span class="pt"></span>';p.appendChild(r);rows.push(r)}}
function fillStand(){
 var ds=B.standings&&B.standings.divisions;if(!ds)return false;
 var dv=ds.filter(function(d){return d.rows.some(function(r){return r.is_ramblers})})[0]||ds[0];if(!dv||!dv.rows.length)return false;
 set('p4tag',dv.name+' standings');
 rows.forEach(function(el,i){var r=dv.rows[i];if(!r){el.style.display='none';return}
  el.style.display='';el.className='row big'+(r.is_ramblers?' me':'');
  var c=el.children;c[0].textContent=r.rank;var im=c[1].firstChild;if(has(r.logo_url)){im.src=r.logo_url;im.style.display=''}else im.style.display='none';
  c[2].textContent=r.abbr;c[3].textContent=r.gp+' GP';c[4].textContent=r.pts})
 return true}
function ranked(n,of){return has(n)?ord(n)+(has(of)?' OF '+of:'')+' IN MHL':''}
function slams(){
 var out=[],t=B.team_stats||{},lr=t.league_ranks||{},g=B.next_game,o=g&&g.opponent;
 if(INS&&!INS.stub&&INS.items){INS.items.slice().sort(function(a,b){return(a.priority||99)-(b.priority||99)}).forEach(function(it){
  if(it.stat&&has(it.stat.value)&&out.length<2)out.push({tag:'Ramblers stat',val:String(it.stat.value),lab:it.stat.label||it.headline,det:has(it.stat.label)?it.headline:it.detail})})}
 var st=t.streak&&t.streak.split('-');
 if(has(t.pk_pct))out.push({tag:'Ramblers special teams',val:t.pk_pct.toFixed(1)+'%',lab:'Penalties killed',det:ranked(lr.pk_pct,lr.of_teams)});
 if(o&&o.record&&o.record.streak){var w=+o.record.streak.split('-')[0];if(w>=2)out.push({tag:'Next up: '+o.abbr,val:String(w),lab:o.abbr+' wins in a row',det:'RECORD '+o.record.overall+' · LAST 10 '+o.record.last10})}
 if(g){var h=g.h2h_last_season;var tl=g.h2h_records&&g.h2h_records.last_season;if(tl&&tl.home_team&&tl.visiting_team){var A=g.home?tl.home_team:tl.visiting_team,Bv=g.home?tl.visiting_team:tl.home_team;
  if(has(A.w)&&has(Bv.w)&&(A.w+Bv.w)>0)out.push({tag:'Head to head · last season',val:A.w+'-'+Bv.w,lab:'Ramblers vs '+o.abbr,det:'Wins last season'})}}
 if(has(t.shots_for_pg))out.push({tag:'Shots on goal',val:t.shots_for_pg.toFixed(1),lab:'Shots for per game',det:t.shots_against_pg?'Shots against: '+t.shots_against_pg.toFixed(1)+' per game':''});
 if(has(t.gf_pg))out.push({tag:'Offence',val:t.gf_pg.toFixed(2),lab:'Goals per game',det:ranked(lr.gf,lr.of_teams)+' · '+t.gf+' goals total'});
 return out.slice(0,2)}
function fillSlam(k,m){var s=slams()[m];if(!s)return false;
 set('p'+k+'tag',s.tag);set('p'+k+'val',s.val);set('p'+k+'lab',s.lab);set('p'+k+'det',s.det);
 var v=$('p'+k+'val');v.className='abs big val'+(s.val.length>5?' xs':s.val.length>3?' sm':'');return true}

var defs=[['p1',fillNext],['p2',fillLast],['p3',fillStar],['p4',fillStand],['p5',function(){return fillSlam(5,0)}],['p6',function(){return fillSlam(6,1)}]];
function rebuild(){
 if(!B)return;
 var cur=panels[idx];
 panels=defs.filter(function(d){try{return d[1]()}catch(e){return false}}).map(function(d){return $(d[0])});
 var ci=panels.indexOf(cur);idx=ci;
 img('tlogo',B.team.logo_url);
 var ts=B.team_stats;set('trec',ts&&ts.record?ts.record+' · '+ts.pts+' PTS':'');
 set('foot',B.copyright||'Official statistics provided by Maritime Hockey League. Powered by HockeyTech.com');
 tickCd()}
function clock(){var d=new Date();set('clock',timeStr(d))}
function next(){
 if(!panels.length)return;
 if(idx>=0&&panels[idx])panels[idx].classList.remove('on');
 idx=(idx+1)%panels.length;panels[idx].classList.add('on');
 var b=$('bar');b.style.transition='none';b.style.transform='scaleX(0)';void b.offsetWidth;b.style.transition='transform '+FRAME_MS+'ms linear';b.style.transform='scaleX(1)'}
function load(){return Promise.all([j('../../data/board.json'),j('../../data/insights.json')]).then(function(r){if(r[0])B=r[0];INS=r[1];rebuild()})}
buildRows();
load().then(function(){next();setInterval(next,FRAME_MS);setInterval(function(){clock();tickCd()},1000);clock();
 setInterval(function(){load()},300000)});
})();
