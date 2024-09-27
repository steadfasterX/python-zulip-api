# Matrix <==> Zulip bridge

This acts as a simple and (non-puppeting) bridge between Matrix and Zulip.

### Features

Currently set of features is quite basic but it's planned to extend it as described.

|state|direction|function|
|-|-|-|
|✅|Matrix => Zulip|send message|
|✅|Zulip => Matrix|send message|
|✅|Matrix => Zulip|media handling|
|✅|Zulip => Matrix|media handling|
|🛠️|Matrix => Zulip|set reactions|
|🛠️|Zulip => Matrix|set reactions|
|🛠️|Matrix => Zulip|encryption|
|🛠️|Zulip => Matrix|encryption|
|🛠️|Matrix => Zulip|emoji + message redaction/delete|
|🛠️|Zulip => Matrix|emoji + message redaction/delete|
|🛠️|Matrix => Zulip|edit messages|
|🛠️|Zulip => Matrix|edit messages|


### Implementation Concept

Zulip uses a "stream+topic" concept which will be translated by this bridge to Matrix as "room+thread".

|Zulip||Matrix|
|-|-|-|
|Project|<=>|Space¹|
|Stream|<=>|Room|
|Topic|<=>|Thread²|

- ¹) *Optional but makes sense if using multiple streams. Client(s) must support this Matrix feature to make use of it. Most modern clients do so though.*
- ²) *Client(s) connecting to a Matrix room HAVE TO support the Matrix threading standard. Most modern clients do so though.*

Zulip makes heavy use of topics - ideally where any user can freely create a topic within a stream as they wish.
A previous release of this bridge was able to handle fixed "stream+topic" combinations only - which in fact is against the Zulip's way of acting (users can/should create new topics at any time).
The bridge still supports that static approach but also allows to bridge **any** topic to a Matrix room - if that feature is enabled.

Note: theoretically you can bridge several Zulip *streams* within the same Matrix *room* but it will become very unreadable usually. So best practice here is to use a single "Matrix room <=> Zulip stream" mapping. Combined with creating a "Space" in Matrix where you can add all Matrix rooms makes the whole Zulip to Matrix feeling perfect.

The configuration file can handle multiple of "stream<->room : topic" combinations so you can run just a single bot for all if you like or 1 bot per combination.

## Installation

Run `pip install -r requirements.txt` in order to install the requirements.

In case you'd like encryption to work, you need pip to install the `matrix-nio`
package with e2e support:
- First, you need to make sure that the development files of the `libolm`
  C-library are installed on your system! See [the corresponding documentation
  of matrix-nio](https://github.com/poljar/matrix-nio#installation) for further
  information on this point.
- `pip install matrix-nio[e2e]`


## Steps to configure the Matrix bridge

To obtain a configuration file template, run the script with the
`--write-sample-config` option to obtain a configuration file to fill in the
details mentioned below. For example:

* If you installed the `zulip` package: `zulip-matrix-bridge --write-sample-config matrix_bridge.conf`

* If you are running from the Zulip GitHub repo: `python matrix_bridge.py --write-sample-config matrix_bridge.conf`

### 1. Zulip endpoint
1. Create a generic Zulip bot, with a full name such as `Matrix Bot`.
2. The bot is able to subscribe to the necessary streams itself if they are
   public. (Note that the bridge will not try to create streams in case they
   do not already exist. In that case, the bridge will fail at startup.)
   Otherwise, you need to add the bot manually.
3. In the `zulip` section of the configuration file, enter the bot's `zuliprc`
   details (`email`, `api_key`, and `site`).
4. In the same section, also enter the Zulip `stream` and `topic`.

### 2. Matrix endpoint
1. Create a user on the matrix server of your choice, e.g. [matrix.org](https://matrix.org/),
   preferably with a descriptive name such as `zulip-bot`.
2. In the `matrix` section of the configuration file, enter the user's Matrix
   user ID `mxid` and password. Please use the Matrix user ID ([MXID](https://matrix.org/faq/#what-is-a-mxid%3F))
   as format for the username!
3. Create the Matrix room(s) to be bridged in case they do not exits yet.
   Remember to invite the bot to private rooms! Otherwise, this error will be
   thrown: `Matrix bridge error: JoinError: M_UNKNOWN No known servers`.
4. Enter the `host` and `room_id` into the same section.
   In case the room is private you need to use the `Internal room ID` which has
   the format `!aBcDeFgHiJkLmNoPqR:example.org`.
   In the official Matrix client [Element](https://github.com/vector-im), you
   can find this `Internal room ID` in the `Room Settings` under `Advanced`.

### Adding more (Zulip topic, Matrix channel)-pairs
1. Create a new section with a name starting with `additional_bridge`.
2. Add a `room_id` for the Matrix side and a `stream` and a `topic` for the
   Zulip side.

Example:
```
[additional_bridge1]
room_id = #zulip:matrix.org
stream = matrix test
topic = matrix test topic
```


## Running the bridge

After the steps above have been completed, assuming you have the configuration
in a file called `matrix_bridge.conf`:

* If you installed the `zulip` package: run `zulip-matrix-bridge -c matrix_bridge.conf`

* If you are running from the Zulip GitHub repo: run `python matrix_bridge.py -c matrix_bridge.conf`

