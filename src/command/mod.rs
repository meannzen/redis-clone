use crate::command::lrange::{BLPop, LLen, LPop};
use crate::command::rpush::LPush;
use crate::command::subscribe::Unsubscribe;
use crate::parse::Parse;
use crate::server::{ReplicaState, Shutdown, TransactionState};
use crate::store::Db;
use crate::{Connection, Frame};

pub mod authentication;
pub mod config;
pub mod discard;
pub mod echo;
pub mod exec;
pub mod geo;
pub mod get;
pub mod incr;
pub mod info;
pub mod key;
pub mod lrange;
pub mod multi;
pub mod ping;
pub mod psync;
pub mod publish;
pub mod replconf;
pub mod rpush;
pub mod set;
pub mod subscribe;
pub mod type_cmd;
pub mod unknown;
pub mod wait;
pub mod watch;
pub mod xadd;
pub mod xrange;
pub mod xread;
pub use config::Config;
pub use discard::Discard;
pub use echo::Echo;
pub use exec::Exec;
pub use get::Get;
pub use incr::Incr;
pub use info::Info;
pub use key::Keys;
pub use lrange::LRange;
pub use multi::Multi;
pub use ping::Ping;
pub use psync::PSync;
pub use publish::Publish;
pub use replconf::ReplConf;
pub use rpush::RPush;
pub use set::Set;
pub use subscribe::Subscribe;
pub use type_cmd::Type;
pub use unknown::Unknown;
pub use wait::Wait;
pub use xadd::XAdd;
pub use xrange::XRange;
pub use xread::XRead;
pub mod zadd;
pub use authentication::{Auth, ACL};
pub use geo::{GeoAdd, GeoDist, GeoPos, GeoSearch};
pub use watch::Watch;
pub use zadd::{ZAdd, ZCard, ZRange, ZRank, ZRem, ZScore};

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Get(Get),
    Set(Set),
    Config(Config),
    Keys(Keys),
    Info(Info),
    ReplConf(ReplConf),
    PSync(PSync),
    Wait(Wait),
    Type(Type),
    XAdd(XAdd),
    XRange(XRange),
    XRead(XRead),
    Ince(Incr),
    Muiti(Multi),
    Exec(Exec),
    Discard(Discard),
    RPush(RPush),
    LRange(LRange),
    LPush(LPush),
    LLen(LLen),
    LPop(LPop),
    BLPop(BLPop),
    Subscribe(Subscribe),
    Publish(Publish),
    Unsubscribe(Unsubscribe),
    ZAdd(ZAdd),
    ZRank(ZRank),
    ZRange(ZRange),
    ZCard(ZCard),
    ZScore(ZScore),
    ZRem(ZRem),
    GeoAdd(GeoAdd),
    GeoPos(GeoPos),
    GeoDis(GeoDist),
    GSearch(GeoSearch),
    ACL(ACL),
    Auth(Auth),
    Unknown(Unknown),
    Watch(Watch),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let command_string = parse.next_string()?.to_lowercase();

        let command = match &command_string[..] {
            "ping" => Command::Ping(Ping::parse_frame(&mut parse)?),
            "echo" => Command::Echo(Echo::parse_frame(&mut parse)?),
            "get" => Command::Get(Get::parse_frame(&mut parse)?),
            "set" => Command::Set(Set::parse_frame(&mut parse)?),
            "keys" => Command::Keys(Keys::parse_frame(&mut parse)?),
            "info" => Command::Info(Info::parse_frame(&mut parse)?),
            "replconf" => Command::ReplConf(ReplConf::parse_frame(&mut parse)?),
            "psync" => Command::PSync(PSync::parse_frame(&mut parse)?),
            "wait" => Command::Wait(Wait::parse_frame(&mut parse)?),
            "type" => Command::Type(Type::parse_frame(&mut parse)?),
            "xadd" => Command::XAdd(XAdd::parse_frame(&mut parse)?),
            "xrange" => Command::XRange(XRange::parse_frame(&mut parse)?),
            "xread" => Command::XRead(XRead::parse_frame(&mut parse)?),
            "incr" => Command::Ince(Incr::parse_frame(&mut parse)?),
            "multi" => Command::Muiti(Multi::parse_frame(&mut parse)?),
            "exec" => Command::Exec(Exec::parse_frame(&mut parse)?),
            "discard" => Command::Discard(Discard::parse_frame(&mut parse)?),
            "rpush" => Command::RPush(RPush::parse_frame(&mut parse)?),
            "lrange" => Command::LRange(LRange::parse_frame(&mut parse)?),
            "lpush" => Command::LPush(LPush::parse_frame(&mut parse)?),
            "llen" => Command::LLen(LLen::parse_frame(&mut parse)?),
            "lpop" => Command::LPop(LPop::parse_frame(&mut parse)?),
            "blpop" => Command::BLPop(BLPop::parse_frame(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frame(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frame(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frame(&mut parse)?),
            "zadd" => Command::ZAdd(ZAdd::parse_frame(&mut parse)?),
            "zrank" => Command::ZRank(ZRank::parse_frame(&mut parse)?),
            "zrange" => Command::ZRange(ZRange::parse_frame(&mut parse)?),
            "zcard" => Command::ZCard(ZCard::parse_frame(&mut parse)?),
            "zscore" => Command::ZScore(ZScore::parse_frame(&mut parse)?),
            "zrem" => Command::ZRem(ZRem::parse_frame(&mut parse)?),
            "geoadd" => Command::GeoAdd(GeoAdd::parse_frame(&mut parse)?),
            "geopos" => Command::GeoPos(GeoPos::parse_frame(&mut parse)?),
            "geodist" => Command::GeoDis(GeoDist::parse_frame(&mut parse)?),
            "geosearch" => Command::GSearch(GeoSearch::parse_frame(&mut parse)?),
            "acl" => Command::ACL(ACL::parse_frame(&mut parse)?),
            "auth" => Command::Auth(Auth::parse_frame(&mut parse)?),
            "config" => {
                let sub_command_string = parse.next_string()?.to_lowercase();
                match &sub_command_string[..] {
                    "get" => Command::Config(Config::parse_frame(&mut parse)?),
                    _ => {
                        return Ok(Command::Unknown(Unknown::new(sub_command_string)));
                    }
                }
            }
            "watch" => Command::Watch(Watch::parse_frame(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_string)));
            }
        };
        parse.finish()?;
        Ok(command)
    }

    pub async fn apply(
        self,
        transaction_state: &TransactionState,
        replica_state: &ReplicaState,
        db: &Db,
        config: &crate::server_cli::Cli,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Auth(cmd) => {
                return cmd.apply(db, conn).await;
            }
            other => {
                if !conn.is_authenticated() {
                    conn.write_frame(&Frame::Error("NOAUTH Authentication required.".to_string()))
                        .await?;
                    return Ok(());
                }

                match other {
                    Ping(cmd) => cmd.apply(conn).await,
                    Echo(cmd) => cmd.apply(conn).await,
                    Get(cmd) => cmd.apply(db, conn, transaction_state).await,
                    Set(cmd) => cmd.apply(db, conn, transaction_state).await,
                    Config(cmd) => cmd.apply(config, conn).await,
                    Keys(cmd) => cmd.apply(db, conn).await,
                    Info(cmd) => cmd.apply(config, conn).await,
                    ReplConf(cmd) => cmd.apply(conn, replica_state).await,
                    PSync(cmd) => cmd.apply(conn, replica_state).await,
                    Wait(cmd) => cmd.apply(conn, replica_state).await,
                    Type(cmd) => cmd.apply(db, conn).await,
                    XAdd(cmd) => cmd.apply(db, conn).await,
                    XRange(cmd) => cmd.apply(db, conn).await,
                    XRead(cmd) => cmd.apply(db, conn).await,
                    Ince(cmd) => cmd.apply(db, conn, transaction_state).await,
                    Muiti(cmd) => cmd.apply(transaction_state, conn).await,
                    Exec(cmd) => cmd.apply(db, transaction_state, conn).await,
                    Discard(cmd) => cmd.apply(conn, transaction_state).await,
                    RPush(cmd) => cmd.apply(db, conn).await,
                    LRange(cmd) => cmd.apply(db, conn).await,
                    LPush(cmd) => cmd.apply(db, conn).await,
                    LLen(cmd) => cmd.apply(db, conn).await,
                    LPop(cmd) => cmd.apply(db, conn).await,
                    BLPop(cmd) => cmd.apply(db, conn).await,
                    Subscribe(cmd) => cmd.apply(db, conn, shutdown).await,
                    Publish(cmd) => cmd.apply(db, conn).await,
                    Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
                    ZAdd(cmd) => cmd.apply(db, conn).await,
                    ZRank(cmd) => cmd.apply(db, conn).await,
                    ZRange(cmd) => cmd.apply(db, conn).await,
                    ZCard(cmd) => cmd.apply(db, conn).await,
                    ZScore(cmd) => cmd.apply(db, conn).await,
                    ZRem(cmd) => cmd.apply(db, conn).await,
                    GeoAdd(cmd) => cmd.apply(db, conn).await,
                    GeoPos(cmd) => cmd.apply(db, conn).await,
                    GeoDis(cmd) => cmd.apply(db, conn).await,
                    GSearch(cmd) => cmd.apply(db, conn).await,
                    ACL(cmd) => cmd.apply(db, conn).await,
                    Unknown(cmd) => cmd.apply(conn).await,
                    Watch(cmd) => cmd.apply(transaction_state, conn).await,
                    _ => Ok(()),
                }
            }
        }
    }

    pub fn is_writer(&self) -> bool {
        matches!(self, Command::Set(_))
    }

    pub fn get_name(&self) -> &str {
        match self {
            Command::Ping(_) => "ping",
            Command::Echo(_) => "echo",
            Command::Get(_) => "get",
            Command::Set(_) => "set",
            Command::Config(_) => "config",
            Command::Keys(_) => "keys",
            Command::Info(_) => "info",
            Command::ReplConf(_) => "replconf",
            Command::PSync(_) => "psync",
            Command::Wait(_) => "wait",
            Command::Type(_) => "type",
            Command::XAdd(_) => "xadd",
            Command::XRange(_) => "xrange",
            Command::XRead(_) => "xread",
            Command::Ince(_) => "incr",
            Command::Muiti(_) => "multi",
            Command::Exec(_) => "exec",
            Command::Discard(_) => "discard",
            Command::RPush(_) => "rpush",
            Command::LRange(_) => "lrange",
            Command::LPush(_) => "lpush",
            Command::LLen(_) => "llen",
            Command::LPop(_) => "lpop",
            Command::BLPop(_) => "blpop",
            Command::Subscribe(_) => "subscribe",
            Command::Publish(_) => "publish",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::ZAdd(_) => "zadd",
            Command::ZRank(_) => "zrank",
            Command::ZRange(_) => "zrange",
            Command::ZCard(_) => "zcard",
            Command::ZScore(_) => "zscore",
            Command::ZRem(_) => "zrem",
            Command::GeoAdd(_) => "geoadd",
            Command::GeoPos(_) => "geopos",
            Command::GeoDis(_) => "geodist",
            Command::GSearch(_) => "gsearch",
            Command::ACL(_) => "acl",
            Command::Auth(_) => "auth",
            Command::Watch(_) => "watch",
            Command::Unknown(_) => "unknown",
        }
    }
}
