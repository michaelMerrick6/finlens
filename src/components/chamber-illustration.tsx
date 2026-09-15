/** Stylized chamber elevation with separated seating blocks; static server SVG. */
export function ChamberIllustration() {
 const rows = [
  { y:154, count:5, step:24, start:96, rise:12 },
  { y:183, count:6, step:25, start:69, rise:16 },
  { y:215, count:7, step:26, start:38, rise:20 },
 ];
 return <svg className="chamber-illustration" viewBox="0 0 500 260" fill="none" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
 <g stroke="#8d9e80" strokeWidth="1.15" strokeLinejoin="round" strokeLinecap="round">
 {/* Architectural backdrop and gallery rail. */}
 <g opacity=".48"><path d="M40 121V39h420v82M34 34h432M49 46h402M49 112h402"/>
 {[64,119,174,310,365,420].map(x=><g key={x}><path d={`M${x} 49v55m7-55v55m-10 1h20m-20-59h20`}/></g>)}
 <path d="M84 60h25v36H84zM139 60h25v36h-25zM336 60h19v36h-19zM391 60h19v36h-19z"/></g>
 {/* Flag behind the speaker's chair. */}
 <g opacity=".72"><path d="M224 43h52v48h-52z" fill="#f4f6ef"/><path d="M224 43h22v21h-22z" fill="#e1e7d9"/>
 {[49,55,61,67,73,79,85].map(y=><path key={y} d={`M${y<65?246:224} ${y}h${y<65?30:52}`}/>)}
 </g>
 {/* Raised rostrum, lectern and clerk's desk. */}
 <g opacity=".85"><path d="M237 103V88q13-6 26 0v15" fill="#e6ebdf"/>
 <path d="M214 104h72v20h-72z" fill="#f4f6ef"/><path d="M210 102h80v5h-80zM222 111h56M225 111v10m50-10v10"/>
 <path d="M193 126h114v16H193z" fill="#edf1e5"/><path d="M189 123h122v5H189zM204 132h92M204 132v7m92-7v7"/>
 <path d="M181 145h138m-145 5h152m-159 5h166"/></g>
 {/* Upright seat backs in shallow perspective, with a clear central aisle. */}
 {rows.map((row,r)=><g key={r} opacity={.58+r*.1}>
 {[false,true].map(right=><g key={String(right)}>{Array.from({length:row.count},(_,i)=>{
 const leftX=row.start+i*row.step;
 const x=right?500-leftX-17:leftX;
 const y=row.y+row.rise*(i/(row.count-1));
 return <g key={i} transform={`translate(${x} ${y})`}>
 <path d="M1 0q7-3 14 0v12H1Z" fill="#e7ecdf"/>
 <path d="M0 12h16l2 5H-2Z" fill="#f4f6ef"/>
 <path d="M0 17v4m16-4v4"/>
 </g>;
 })}</g>)}
 </g>)}
 <g opacity=".3"><path d="m236 162-10 88m38-88 10 88M29 248h185m72 0h185"/></g>
 </g></svg>;
}
