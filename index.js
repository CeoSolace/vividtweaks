require("dotenv").config();

const {
  Client,
  GatewayIntentBits,
  SlashCommandBuilder,
  Routes,
  REST,
  PermissionsBitField,
  ChannelType,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  StringSelectMenuBuilder,
  StringSelectMenuOptionBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
} = require("discord.js");

const express = require("express");
const Stripe = require("stripe");
const { MongoClient, ObjectId } = require("mongodb");
const discordTranscripts = require("discord-html-transcripts");

// ===================== SERVER LOCK + CHANNELS =====================
const ALLOWED_GUILD_ID = "1269702895549419544";

// Panel channels
const PURCHASE_PANEL_CHANNEL_ID = "1452386415814901924";
const SUPPORT_PANEL_CHANNEL_ID = "000000000000000000"; // <- SET THIS

// Logs
const REFERENCE_LOG_CHANNEL_ID = "1452429835677728975"; // optional
const PURCHASE_LOG_FALLBACK_NAME = "purchase-log";

// Refund approvals
const REFUND_APPROVER_USER_ID = "1400281740978815118";

// ===================== BRANDING =====================
const BRAND_NAME = "Vivid Tweaks";
const BRAND_COLOR = 0x8b5cf6;
const BRAND_FOOTER = "Vivid Tweaks";

// ===================== REFERENCE CODES =====================
const VALID_REFERENCE_CODES = new Set(["synex"]); // lowercase

// ===================== MONEY/PLANS =====================
const CURRENCY = "gbp";
const REFUND_WINDOW_MS = 24 * 60 * 60 * 1000;

const PLAN_KEYS = ["one_time", "monthly", "annual", "lifetime"];
const PLAN_LABELS = {
  one_time: "One-time",
  monthly: "Monthly",
  annual: "Annually",
  lifetime: "Lifetime",
};
const PLAN_INTERVAL = {
  monthly: { interval: "month", interval_count: 1 },
  annual: { interval: "year", interval_count: 1 },
};

// ===================== ENV =====================
const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const CLIENT_ID = process.env.DISCORD_CLIENT_ID;
const MONGODB_URI = process.env.MONGODB_URI;

const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY;
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET;

const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL;
const PORT = Number(process.env.PORT || 3000);

if (!DISCORD_TOKEN) throw new Error("Missing DISCORD_TOKEN");
if (!CLIENT_ID) throw new Error("Missing DISCORD_CLIENT_ID");
if (!MONGODB_URI) throw new Error("Missing MONGODB_URI");
if (!STRIPE_SECRET_KEY) throw new Error("Missing STRIPE_SECRET_KEY");
if (!STRIPE_WEBHOOK_SECRET) throw new Error("Missing STRIPE_WEBHOOK_SECRET");
if (!PUBLIC_BASE_URL) throw new Error("Missing PUBLIC_BASE_URL");

// ===================== STRIPE =====================
const stripe = new Stripe(STRIPE_SECRET_KEY, { apiVersion: "2024-06-20" });

// ===================== HELPERS =====================
function amountToMinorUnits(amountStr) {
  if (typeof amountStr !== "string") return null;
  const t = amountStr.trim();
  if (!/^\d+(\.\d{1,2})?$/.test(t)) return null;
  const [whole, fracRaw] = t.split(".");
  const frac = (fracRaw || "").padEnd(2, "0").slice(0, 2);
  const minor = Number(whole) * 100 + Number(frac);
  if (!Number.isFinite(minor) || minor <= 0) return null;
  return minor;
}

function minorToDisplay(minor) {
  return `£${(minor / 100).toFixed(2)}`;
}

function makeId(prefix) {
  return `${prefix}-${Date.now().toString(36).toUpperCase()}-${Math.random()
    .toString(36)
    .slice(2, 7)
    .toUpperCase()}`;
}
const makePurchaseId = () => makeId("VT");
const makeRefundRequestId = () => makeId("RR");

function enabledPlans(pricesObj) {
  const out = [];
  for (const key of PLAN_KEYS) if (pricesObj?.[key]?.amountMinor) out.push(key);
  return out;
}

function formatPlans(pricesObj) {
  const plans = enabledPlans(pricesObj);
  if (!plans.length) return "None";
  return plans
    .map((k) => `\`${PLAN_LABELS[k]} ${minorToDisplay(pricesObj[k].amountMinor)}\``)
    .join(", ");
}

// cooldown so humans can’t spam 99 checkout links/sec
const ACTION_COOLDOWN_MS = 2000;
const cooldown = new Map();
function isCooling(key) {
  const now = Date.now();
  const last = cooldown.get(key) || 0;
  if (now - last < ACTION_COOLDOWN_MS) return true;
  cooldown.set(key, now);
  return false;
}

async function dmUser(userId, payload) {
  try {
    const user = await client.users.fetch(userId).catch(() => null);
    if (!user) return false;
    await user.send(payload).catch(() => {});
    return true;
  } catch {
    return false;
  }
}

// HTML transcript attachment
async function buildTranscriptAttachment(channel) {
  const filename = `transcript-${channel.name}-${channel.id}.html`.slice(0, 120);
  const attachment = await discordTranscripts.createTranscript(channel, {
    limit: -1,
    returnType: "attachment",
    filename,
    saveImages: true,
    poweredBy: false,
  });
  return { attachment, filename };
}

// ===================== DB =====================
const mongo = new MongoClient(MONGODB_URI, {
  maxPoolSize: 5,
  minPoolSize: 0,
  serverSelectionTimeoutMS: 5000,
  connectTimeoutMS: 8000,
});

let db;
let productsCol, configCol, purchasesCol, refundRequestsCol, ticketsCol, entitlementsCol;

async function initDb() {
  await mongo.connect();
  db = mongo.db();

  productsCol = db.collection("products");
  configCol = db.collection("shop_config");
  purchasesCol = db.collection("purchases");
  refundRequestsCol = db.collection("refund_requests");
  ticketsCol = db.collection("tickets");
  entitlementsCol = db.collection("entitlements"); // anti-doublebuy + upgrades

  await Promise.allSettled([
    productsCol.createIndex({ guildId: 1, createdAt: -1 }),
    configCol.createIndex({ guildId: 1 }, { unique: true }),

    purchasesCol.createIndex({ purchaseId: 1 }, { unique: true }),
    purchasesCol.createIndex({ stripeSessionId: 1 }, { unique: true, sparse: true }),
    purchasesCol.createIndex({ stripeSubscriptionId: 1 }, { sparse: true }),
    purchasesCol.createIndex({ guildId: 1, paidAt: -1 }),

    refundRequestsCol.createIndex({ requestId: 1 }, { unique: true }),
    refundRequestsCol.createIndex({ guildId: 1, createdAt: -1 }),
    refundRequestsCol.createIndex({ status: 1, createdAt: -1 }),

    ticketsCol.createIndex({ guildId: 1, userId: 1, status: 1, kind: 1 }),
    ticketsCol.createIndex({ guildId: 1, channelId: 1 }, { unique: true }),

    entitlementsCol.createIndex({ guildId: 1, userId: 1, productId: 1 }, { unique: true }),
    entitlementsCol.createIndex({ guildId: 1, userId: 1, status: 1 }),
  ]);

  console.log("✅ MongoDB connected");
}

// ===================== CONFIG CACHE =====================
const CONFIG_CACHE_TTL_MS = 30_000;
const configCache = new Map();

async function getConfig(guildId) {
  const now = Date.now();
  const cached = configCache.get(guildId);
  if (cached && cached.exp > now) return cached.doc;
  const doc = (await configCol.findOne({ guildId })) || null;
  configCache.set(guildId, { doc, exp: now + CONFIG_CACHE_TTL_MS });
  return doc;
}

async function upsertConfig(guildId, patch) {
  await configCol.updateOne(
    { guildId },
    { $set: { ...patch, updatedAt: new Date() } },
    { upsert: true }
  );
  configCache.delete(guildId);
}

// ===================== DISCORD =====================
const client = new Client({ intents: [GatewayIntentBits.Guilds] });

function ensureAllowedGuild(interaction) {
  if (!interaction.guild || interaction.guild.id !== ALLOWED_GUILD_ID) {
    interaction.reply({ content: "Bot is locked to one server. Not this one.", ephemeral: true }).catch(() => {});
    return false;
  }
  return true;
}

function isAdmin(interaction) {
  const member = interaction.member;
  return (
    member?.permissions &&
    new PermissionsBitField(member.permissions).has(PermissionsBitField.Flags.Administrator)
  );
}

function requireAdmin(interaction) {
  if (!isAdmin(interaction)) {
    interaction.reply({ content: "Administrator permission required.", ephemeral: true }).catch(() => {});
    return false;
  }
  return true;
}

// ===================== CHANNEL HELPERS =====================
async function ensurePurchaseLogChannel(guild) {
  const cfg = await getConfig(guild.id);

  if (cfg?.purchaseLogChannelId) {
    const ch = await guild.channels.fetch(cfg.purchaseLogChannelId).catch(() => null);
    if (ch && ch.type === ChannelType.GuildText) return ch;
  }

  const existing = guild.channels.cache.find(
    (c) => c.type === ChannelType.GuildText && c.name === PURCHASE_LOG_FALLBACK_NAME
  );
  if (existing) {
    await upsertConfig(guild.id, { purchaseLogChannelId: existing.id });
    return existing;
  }

  const created = await guild.channels.create({
    name: PURCHASE_LOG_FALLBACK_NAME,
    type: ChannelType.GuildText,
    permissionOverwrites: [
      { id: guild.roles.everyone.id, deny: [PermissionsBitField.Flags.ViewChannel] },
    ],
  });

  await upsertConfig(guild.id, { purchaseLogChannelId: created.id });
  return created;
}

async function logToPurchaseLog(guild, embed, components = [], files = []) {
  const ch = await ensurePurchaseLogChannel(guild);
  await ch.send({ embeds: [embed], components, files }).catch(() => {});
}

async function getReferenceLogChannel(guild) {
  const ch = await guild.channels.fetch(REFERENCE_LOG_CHANNEL_ID).catch(() => null);
  if (!ch || ch.type !== ChannelType.GuildText) return null;
  return ch;
}

// ===================== ENTITLEMENTS (ANTI-DOUBLEBUY + UPGRADES) =====================
async function getEntitlement(guildId, userId, productId) {
  return (await entitlementsCol.findOne({ guildId, userId, productId })) || null;
}

// user can’t purchase same product twice,
// except: upgrade one_time -> monthly/annual
async function canStartCheckout({ guildId, userId, product, planKey, isUpgrade }) {
  const ent = await getEntitlement(guildId, userId, product._id.toString());
  if (!ent || ent.status !== "active") return { ok: true, reason: null };

  if (ent.planKey === "lifetime") {
    return { ok: false, reason: "You already own **Lifetime** for this product." };
  }

  if (isUpgrade) {
    if (ent.planKey !== "one_time") return { ok: false, reason: "Upgrade is only for **One-time → Monthly/Annual**." };
    if (!(planKey === "monthly" || planKey === "annual")) return { ok: false, reason: "Upgrade must be **Monthly** or **Annual**." };
    if (ent.stripeSubscriptionId) return { ok: false, reason: "A subscription is already on record for this product." };
    return { ok: true, reason: null };
  }

  return {
    ok: false,
    reason:
      `You already own this product (**${PLAN_LABELS[ent.planKey] || ent.planKey}**). ` +
      `If you bought one-time and want updates, use **/upgrade**.`,
  };
}

// ===================== TICKETS =====================
async function ensureCategory(guild, name) {
  const existing = guild.channels.cache.find((c) => c.type === ChannelType.GuildCategory && c.name === name);
  if (existing) return existing;
  return guild.channels.create({ name, type: ChannelType.GuildCategory });
}

function disableComponents(components) {
  const out = [];
  for (const row of components || []) {
    const newRow = new ActionRowBuilder();
    for (const comp of row.components || []) {
      if (comp.type === 2) newRow.addComponents(ButtonBuilder.from(comp).setDisabled(true));
      else if (comp.type === 3) newRow.addComponents(StringSelectMenuBuilder.from(comp).setDisabled(true));
    }
    if (newRow.components.length) out.push(newRow);
  }
  return out;
}

async function createOrGetTicket({ guild, userId, kind }) {
  const existing = await ticketsCol.findOne({ guildId: guild.id, userId, status: "open", kind });
  if (existing?.channelId) {
    const ch = await guild.channels.fetch(existing.channelId).catch(() => null);
    if (ch && ch.type === ChannelType.GuildText) return { channel: ch, ticket: existing };
    await ticketsCol.updateOne({ _id: existing._id }, { $set: { status: "stale", staleAt: new Date() } });
  }

  const cfg = await getConfig(guild.id);
  const supportRoleId = cfg?.supportRoleId || null;

  const category = await ensureCategory(guild, kind === "purchase" ? "vivid-purchase" : "vivid-support");

  const overwrites = [
    { id: guild.roles.everyone.id, deny: [PermissionsBitField.Flags.ViewChannel] },
    {
      id: userId,
      allow: [
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.ReadMessageHistory,
      ],
    },
    {
      id: client.user.id,
      allow: [
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.ReadMessageHistory,
        PermissionsBitField.Flags.ManageChannels,
      ],
    },
  ];

  if (supportRoleId) {
    overwrites.push({
      id: supportRoleId,
      allow: [
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.ReadMessageHistory,
      ],
    });
  }

  const name = kind === "purchase" ? `purchase-${userId}` : `support-${userId}`;

  const ch = await guild.channels.create({
    name: name.slice(0, 90),
    type: ChannelType.GuildText,
    parent: category.id,
    permissionOverwrites: overwrites,
  });

  const base = {
    guildId: guild.id,
    channelId: ch.id,
    userId,
    kind,
    status: "open",
    createdAt: new Date(),
    referenceCode: null,
    ticketPanelMessageId: null,
    ticketProductId: null,
  };

  const doc = await ticketsCol.insertOne(base);
  return { channel: ch, ticket: { ...base, _id: doc.insertedId } };
}

async function canManageTicket(interaction, ticketDoc) {
  if (!ticketDoc) return false;
  if (interaction.user.id === ticketDoc.userId) return true;
  if (isAdmin(interaction)) return true;

  const cfg = await getConfig(interaction.guild.id);
  if (cfg?.supportRoleId) {
    const member = await interaction.guild.members.fetch(interaction.user.id).catch(() => null);
    if (member?.roles?.cache?.has(cfg.supportRoleId)) return true;
  }
  return false;
}

async function closeAndDeleteTicketChannel({ interaction, ticketDoc, reason }) {
  const guild = interaction.guild;
  const channel = await guild.channels.fetch(ticketDoc.channelId).catch(() => null);
  if (!channel || channel.type !== ChannelType.GuildText) return;

  // disable UI (best effort)
  try {
    if (ticketDoc.ticketPanelMessageId) {
      const msg = await channel.messages.fetch(ticketDoc.ticketPanelMessageId).catch(() => null);
      if (msg) await msg.edit({ components: disableComponents(msg.components) }).catch(() => {});
    }
  } catch {}

  let transcriptAttachment = null;
  try {
    const { attachment } = await buildTranscriptAttachment(channel);
    transcriptAttachment = attachment;
  } catch (e) {
    console.error("Transcript failed:", e);
  }

  await ticketsCol.updateOne(
    { guildId: guild.id, channelId: ticketDoc.channelId, status: "open" },
    { $set: { status: "closed", closedAt: new Date(), closedBy: interaction.user.id, closeReason: reason || null } }
  ).catch(() => {});

  // DM owner transcript (HTML file)
  const dmText =
    `Your **${BRAND_NAME}** ${ticketDoc.kind} ticket was closed.\n` +
    `Reason: ${reason || "n/a"}\n` +
    `Transcript: attached`;

  if (transcriptAttachment) {
    await dmUser(ticketDoc.userId, { content: dmText, files: [transcriptAttachment] });
  } else {
    await dmUser(ticketDoc.userId, { content: `${dmText}\n(Transcript unavailable)` });
  }

  // log to staff
  const embed = new EmbedBuilder()
    .setColor(BRAND_COLOR)
    .setTitle(`${BRAND_NAME} • Ticket Closed`)
    .setDescription(
      `Kind: **${ticketDoc.kind}**\n` +
      `Owner: <@${ticketDoc.userId}>\n` +
      `Closed by: <@${interaction.user.id}>\n` +
      `Reason: ${reason || "n/a"}\n` +
      `Transcript: ${transcriptAttachment ? "attached (html)" : "`unavailable`"}\n` +
      `Channel: \`${channel.name}\``
    )
    .setFooter({ text: BRAND_FOOTER })
    .setTimestamp(new Date());

  await logToPurchaseLog(guild, embed, [], transcriptAttachment ? [transcriptAttachment] : []).catch(() => {});

  await interaction.editReply("Ticket closed. Deleting channel...").catch(() => {});
  setTimeout(async () => {
    const ch = await guild.channels.fetch(ticketDoc.channelId).catch(() => null);
    if (ch) await ch.delete(reason || "Ticket closed").catch(() => {});
  }, 1500);
}

// ===================== PANELS =====================
async function buildPurchasePanelPayload(guildId) {
  const products = await productsCol.find({ guildId }).sort({ createdAt: -1 }).limit(25).toArray();

  const embed = new EmbedBuilder()
    .setColor(BRAND_COLOR)
    .setTitle(`${BRAND_NAME} • Purchase`)
    .setDescription(
      products.length
        ? "Select what you want to buy. A private purchase ticket will be created."
        : "No products yet. Admins: /addproduct"
    )
    .setFooter({ text: BRAND_FOOTER });

  const components = [];

  if (products.length) {
    const menu = new StringSelectMenuBuilder()
      .setCustomId("purchase_product_select")
      .setPlaceholder("Select a product")
      .setMinValues(1)
      .setMaxValues(1);

    for (const p of products) {
      menu.addOptions(
        new StringSelectMenuOptionBuilder()
          .setLabel(p.name.slice(0, 100))
          .setValue(p._id.toString())
          .setDescription((p.description || "Purchase").slice(0, 100))
      );
    }

    components.push(new ActionRowBuilder().addComponents(menu));
  }

  return { embeds: [embed], components };
}

async function buildSupportPanelPayload() {
  const embed = new EmbedBuilder()
    .setColor(BRAND_COLOR)
    .setTitle(`${BRAND_NAME} • Support`)
    .setDescription("Need help? Open a private support ticket.")
    .setFooter({ text: BRAND_FOOTER });

  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId("support:open").setStyle(ButtonStyle.Primary).setLabel("Open Support Ticket")
  );

  return { embeds: [embed], components: [row] };
}

async function upsertPanelMessage({ guild, channelId, configKey, payloadBuilder }) {
  const ch = await guild.channels.fetch(channelId).catch(() => null);
  if (!ch || ch.type !== ChannelType.GuildText) return;

  const payload = await payloadBuilder();
  const cfg = await getConfig(guild.id);

  const msgId = cfg?.[configKey];
  if (msgId) {
    const msg = await ch.messages.fetch(msgId).catch(() => null);
    if (msg) {
      await msg.edit(payload).catch(() => {});
      return;
    }
  }

  const sent = await ch.send(payload).catch(() => null);
  if (sent?.id) await upsertConfig(guild.id, { [configKey]: sent.id });
}

async function upsertPanels(guild) {
  await upsertPanelMessage({
    guild,
    channelId: PURCHASE_PANEL_CHANNEL_ID,
    configKey: "purchasePanelMessageId",
    payloadBuilder: () => buildPurchasePanelPayload(guild.id),
  });

  await upsertPanelMessage({
    guild,
    channelId: SUPPORT_PANEL_CHANNEL_ID,
    configKey: "supportPanelMessageId",
    payloadBuilder: () => buildSupportPanelPayload(),
  });
}

// ===================== PURCHASE TICKET UI =====================
function purchaseUtilityButtons() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId("ref:add").setStyle(ButtonStyle.Secondary).setLabel("Add Reference Code"),
      new ButtonBuilder().setCustomId("ticket:close").setStyle(ButtonStyle.Danger).setLabel("Close Ticket")
    ),
  ];
}

function supportUtilityButtons() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId("ticket:close").setStyle(ButtonStyle.Danger).setLabel("Close Ticket")
    ),
  ];
}

function planButtons(product) {
  const plans = enabledPlans(product.prices);
  if (!plans.length) return [];
  const row = new ActionRowBuilder();
  for (const planKey of plans) {
    row.addComponents(
      new ButtonBuilder()
        .setCustomId(`buy:${product._id.toString()}:${planKey}`)
        .setStyle(ButtonStyle.Success)
        .setLabel(`${PLAN_LABELS[planKey]} ${minorToDisplay(product.prices[planKey].amountMinor)}`)
    );
  }
  return [row];
}

function upgradeButtons(product) {
  const row = new ActionRowBuilder();
  let added = false;

  if (product?.prices?.monthly?.amountMinor) {
    row.addComponents(
      new ButtonBuilder()
        .setCustomId(`upgrade:${product._id.toString()}:monthly`)
        .setStyle(ButtonStyle.Primary)
        .setLabel(`Upgrade → Monthly ${minorToDisplay(product.prices.monthly.amountMinor)}`)
    );
    added = true;
  }

  if (product?.prices?.annual?.amountMinor) {
    row.addComponents(
      new ButtonBuilder()
        .setCustomId(`upgrade:${product._id.toString()}:annual`)
        .setStyle(ButtonStyle.Primary)
        .setLabel(`Upgrade → Annual ${minorToDisplay(product.prices.annual.amountMinor)}`)
    );
    added = true;
  }

  return added ? [row] : [];
}

function buildPurchaseTicketEmbed({ userId, product, ticket, entitlement }) {
  const ref = ticket?.referenceCode ? `\`${ticket.referenceCode}\`` : "`none`";
  const ownedText =
    entitlement?.status === "active"
      ? `\n**Owned:** ${PLAN_LABELS[entitlement.planKey] || entitlement.planKey}`
      : "";

  const upsell =
    entitlement?.status === "active" && entitlement.planKey === "one_time"
      ? `\n\n🛠️ **Want updates/support?** Upgrade to **Monthly** to keep getting new versions.`
      : "";

  return new EmbedBuilder()
    .setColor(BRAND_COLOR)
    .setTitle(`${BRAND_NAME} • Purchase Ticket`)
    .setDescription(
      `User: <@${userId}>\n` +
        `Product: **${product.name}**\n` +
        `${product.description}\n\n` +
        `Reference Code: ${ref}${ownedText}${upsell}\n\n` +
        `Choose a plan to generate your Stripe checkout link.`
    )
    .addFields({ name: "Plans", value: formatPlans(product.prices), inline: false })
    .setFooter({ text: BRAND_FOOTER })
    .setTimestamp(new Date());
}

function buildSupportTicketEmbed({ userId }) {
  return new EmbedBuilder()
    .setColor(BRAND_COLOR)
    .setTitle(`${BRAND_NAME} • Support Ticket`)
    .setDescription(`User: <@${userId}>\n\nExplain your issue and support will reply.`)
    .setFooter({ text: BRAND_FOOTER })
    .setTimestamp(new Date());
}

// ===================== STRIPE CHECKOUT =====================
function makeLineItemForProduct(product, planKey) {
  const plan = product.prices?.[planKey];
  if (!plan?.amountMinor) throw new Error("Plan not available");

  const isSub = planKey === "monthly" || planKey === "annual";
  const recurring = isSub ? PLAN_INTERVAL[planKey] : undefined;

  return {
    price_data: {
      currency: CURRENCY,
      unit_amount: plan.amountMinor,
      product_data: {
        name: `${BRAND_NAME} • ${product.name} (${PLAN_LABELS[planKey]})`,
        description: product.description,
      },
      ...(recurring ? { recurring } : {}),
    },
    quantity: 1,
  };
}

async function createCheckoutSessionForProduct({ guildId, userId, product, planKey, purchaseId, referenceCode, modeTag }) {
  const isSubscription = planKey === "monthly" || planKey === "annual";
  const lineItem = makeLineItemForProduct(product, planKey);

  const session = await stripe.checkout.sessions.create({
    mode: isSubscription ? "subscription" : "payment",
    line_items: [lineItem],
    success_url: `${PUBLIC_BASE_URL}/success`,
    cancel_url: `${PUBLIC_BASE_URL}/cancel`,
    metadata: {
      purchase_id: purchaseId,
      guild_id: guildId,
      user_id: userId,
      type: "product",
      product_id: product._id.toString(),
      product_name: product.name,
      plan_key: planKey,
      role_id: product.roleId,
      amount_minor: String(product.prices[planKey].amountMinor),
      currency: CURRENCY,
      reference_code: referenceCode || "none",
      mode_tag: modeTag || "purchase",
    },
  });

  await purchasesCol.insertOne({
    purchaseId,
    guildId,
    userId,
    type: "product",
    productId: product._id.toString(),
    productName: product.name,
    planKey,
    roleId: product.roleId,
    amountMinor: product.prices[planKey].amountMinor,
    currency: CURRENCY,
    referenceCode: referenceCode || null,
    stripeSessionId: session.id,
    status: "created",
    createdAt: new Date(),
  });

  return session.url;
}

// ===================== REFUNDS =====================
async function executeRefundInternal(purchase) {
  let refundId = null;

  if (purchase.stripePaymentIntentId) {
    const refund = await stripe.refunds.create({ payment_intent: purchase.stripePaymentIntentId });
    refundId = refund.id;
  } else if (purchase.stripeSubscriptionId) {
    const sub = await stripe.subscriptions.retrieve(purchase.stripeSubscriptionId, {
      expand: ["latest_invoice.payment_intent"],
    });

    const pi = sub.latest_invoice?.payment_intent;
    if (pi?.id) {
      const refund = await stripe.refunds.create({ payment_intent: pi.id });
      refundId = refund.id;
    }
    await stripe.subscriptions.cancel(purchase.stripeSubscriptionId).catch(() => {});
  } else {
    throw new Error("No Stripe payment references on record.");
  }

  return refundId;
}

// ===================== COMMANDS =====================
const commands = [
  new SlashCommandBuilder()
    .setName("addproduct")
    .setDescription("Add a product (amounts, not Stripe price IDs)")
    .addStringOption((o) => o.setName("name").setDescription("Product name").setRequired(true))
    .addStringOption((o) => o.setName("description").setDescription("Short description").setRequired(true))
    .addRoleOption((o) => o.setName("role").setDescription("Role to grant after payment").setRequired(true))
    .addStringOption((o) => o.setName("one_time").setDescription("One-time amount (e.g. 50)"))
    .addStringOption((o) => o.setName("monthly").setDescription("Monthly amount (e.g. 15)"))
    .addStringOption((o) => o.setName("annual").setDescription("Annual amount (e.g. 150)"))
    .addStringOption((o) => o.setName("lifetime").setDescription("Lifetime amount (e.g. 300)")),

  new SlashCommandBuilder()
    .setName("setprice")
    .setDescription("Set/disable a plan amount on an existing product")
    .addStringOption((o) => o.setName("product_id").setDescription("Product ID").setRequired(true))
    .addStringOption((o) =>
      o.setName("plan").setDescription("Plan").setRequired(true).addChoices(
        { name: "one_time", value: "one_time" },
        { name: "monthly", value: "monthly" },
        { name: "annual", value: "annual" },
        { name: "lifetime", value: "lifetime" }
      )
    )
    .addStringOption((o) => o.setName("amount").setDescription("Amount (e.g. 15) or 'none'").setRequired(true)),

  new SlashCommandBuilder().setName("listproducts").setDescription("List product IDs and enabled plans"),

  new SlashCommandBuilder()
    .setName("setsupportrole")
    .setDescription("Set support role for ticket channels (optional)")
    .addRoleOption((o) => o.setName("role").setDescription("Support role").setRequired(true)),

  new SlashCommandBuilder()
    .setName("panel")
    .setDescription("Repost/update purchase & support panels"),

  new SlashCommandBuilder()
    .setName("upgrade")
    .setDescription("Upgrade a one-time purchase to a subscription (monthly/annual)")
    .addStringOption((o) => o.setName("product_id").setDescription("Product ID").setRequired(true))
    .addStringOption((o) =>
      o.setName("to").setDescription("Target plan").setRequired(true).addChoices(
        { name: "monthly", value: "monthly" },
        { name: "annual", value: "annual" }
      )
    ),
].map((c) => c.toJSON());

async function registerCommands() {
  const rest = new REST({ version: "10" }).setToken(DISCORD_TOKEN);
  await rest.put(Routes.applicationGuildCommands(CLIENT_ID, ALLOWED_GUILD_ID), { body: commands });
  console.log("✅ Guild slash commands registered");
}

// ===================== INTERACTIONS =====================
client.on("interactionCreate", async (interaction) => {
  try {
    // Slash Commands
    if (interaction.isChatInputCommand()) {
      if (!ensureAllowedGuild(interaction)) return;

      if (interaction.commandName === "addproduct") {
        if (!requireAdmin(interaction)) return;

        const name = interaction.options.getString("name", true);
        const description = interaction.options.getString("description", true);
        const role = interaction.options.getRole("role", true);

        const raw = {
          one_time: interaction.options.getString("one_time"),
          monthly: interaction.options.getString("monthly"),
          annual: interaction.options.getString("annual"),
          lifetime: interaction.options.getString("lifetime"),
        };

        const prices = {};
        for (const k of PLAN_KEYS) {
          if (!raw[k]) continue;
          const minor = amountToMinorUnits(raw[k]);
          if (!minor) return interaction.reply({ content: `Invalid amount for ${k}: "${raw[k]}"`, ephemeral: true });
          prices[k] = { amountMinor: minor };
        }

        if (!enabledPlans(prices).length) {
          return interaction.reply({ content: "Set at least one plan amount.", ephemeral: true });
        }

        const doc = await productsCol.insertOne({
          guildId: interaction.guild.id,
          name,
          description,
          roleId: role.id,
          prices,
          createdAt: new Date(),
        });

        await upsertPanels(interaction.guild).catch(() => {});
        return interaction.reply({
          content: `Added **${name}** (ID: \`${doc.insertedId.toString()}\`) with: ${formatPlans(prices)}`,
          ephemeral: true,
        });
      }

      if (interaction.commandName === "setprice") {
        if (!requireAdmin(interaction)) return;

        const productId = interaction.options.getString("product_id", true);
        const plan = interaction.options.getString("plan", true);
        const amountRaw = interaction.options.getString("amount", true).trim().toLowerCase();

        if (!ObjectId.isValid(productId)) return interaction.reply({ content: "Invalid product id.", ephemeral: true });

        const product = await productsCol.findOne({ _id: new ObjectId(productId), guildId: interaction.guild.id });
        if (!product) return interaction.reply({ content: "Product not found.", ephemeral: true });

        if (amountRaw === "none" || amountRaw === "disable" || amountRaw === "off") {
          await productsCol.updateOne({ _id: new ObjectId(productId) }, { $unset: { [`prices.${plan}`]: "" } });
        } else {
          const minor = amountToMinorUnits(amountRaw);
          if (!minor) return interaction.reply({ content: "Invalid amount format. Example: 15 or 15.00", ephemeral: true });
          await productsCol.updateOne(
            { _id: new ObjectId(productId) },
            { $set: { [`prices.${plan}`]: { amountMinor: minor } } }
          );
        }

        await upsertPanels(interaction.guild).catch(() => {});
        return interaction.reply({ content: `Updated plan **${plan}** for \`${productId}\`.`, ephemeral: true });
      }

      if (interaction.commandName === "listproducts") {
        if (!requireAdmin(interaction)) return;

        const products = await productsCol.find({ guildId: interaction.guild.id }).sort({ createdAt: -1 }).limit(50).toArray();
        if (!products.length) return interaction.reply({ content: "No products.", ephemeral: true });

        const lines = products.map(
          (p) => `• **${p.name}** \`${p._id}\` | ${formatPlans(p.prices)} | role <@&${p.roleId}>`
        );
        return interaction.reply({ content: lines.join("\n").slice(0, 1900), ephemeral: true });
      }

      if (interaction.commandName === "setsupportrole") {
        if (!requireAdmin(interaction)) return;
        const role = interaction.options.getRole("role", true);
        await upsertConfig(interaction.guild.id, { supportRoleId: role.id });
        return interaction.reply({ content: `Support role set to <@&${role.id}>`, ephemeral: true });
      }

      if (interaction.commandName === "panel") {
        if (!requireAdmin(interaction)) return;
        await upsertPanels(interaction.guild).catch(() => {});
        return interaction.reply({ content: "Panels updated.", ephemeral: true });
      }

      if (interaction.commandName === "upgrade") {
        if (isCooling(`upgrade:${interaction.user.id}`)) {
          return interaction.reply({ content: "Slow down.", ephemeral: true });
        }

        const productId = interaction.options.getString("product_id", true);
        const target = interaction.options.getString("to", true);

        if (!ObjectId.isValid(productId)) return interaction.reply({ content: "Invalid product ID.", ephemeral: true });

        const product = await productsCol.findOne({ _id: new ObjectId(productId), guildId: interaction.guild.id });
        if (!product) return interaction.reply({ content: "Product not found.", ephemeral: true });
        if (!product.prices?.[target]?.amountMinor) return interaction.reply({ content: "That subscription plan is not enabled for this product.", ephemeral: true });

        const ent = await getEntitlement(interaction.guild.id, interaction.user.id, productId);
        if (!ent || ent.status !== "active" || ent.planKey !== "one_time") {
          return interaction.reply({ content: "You can only upgrade if you own **One-time** for this product.", ephemeral: true });
        }

        const gate = await canStartCheckout({
          guildId: interaction.guild.id,
          userId: interaction.user.id,
          product,
          planKey: target,
          isUpgrade: true,
        });

        if (!gate.ok) return interaction.reply({ content: gate.reason, ephemeral: true });

        const purchaseId = makePurchaseId();
        const url = await createCheckoutSessionForProduct({
          guildId: interaction.guild.id,
          userId: interaction.user.id,
          product,
          planKey: target,
          purchaseId,
          referenceCode: ent.referenceCode || null,
          modeTag: "upgrade",
        });

        return interaction.reply({
          content: `Upgrade checkout link (Purchase ID: \`${purchaseId}\`):\n${url}`,
          ephemeral: true,
        });
      }
    }

    // Purchase panel select
    if (interaction.isStringSelectMenu()) {
      if (!ensureAllowedGuild(interaction)) return;

      if (interaction.customId === "purchase_product_select") {
        const productId = interaction.values[0];
        if (!ObjectId.isValid(productId)) return interaction.reply({ content: "Invalid product selection.", ephemeral: true });

        const product = await productsCol.findOne({ _id: new ObjectId(productId), guildId: interaction.guild.id });
        if (!product) return interaction.reply({ content: "Product not found.", ephemeral: true });

        const { channel, ticket } = await createOrGetTicket({ guild: interaction.guild, userId: interaction.user.id, kind: "purchase" });

        await ticketsCol.updateOne({ _id: ticket._id }, { $set: { ticketProductId: productId } }).catch(() => {});

        const entitlement = await getEntitlement(interaction.guild.id, interaction.user.id, productId);

        const embed = buildPurchaseTicketEmbed({ userId: interaction.user.id, product, ticket, entitlement });
        const components = [
          ...purchaseUtilityButtons(),
          ...planButtons(product),
          ...(entitlement?.planKey === "one_time" ? upgradeButtons(product) : []),
        ];

        const msg = await channel.send({ content: `<@${interaction.user.id}>`, embeds: [embed], components }).catch(() => null);
        if (msg?.id) await ticketsCol.updateOne({ _id: ticket._id }, { $set: { ticketPanelMessageId: msg.id } }).catch(() => {});

        return interaction.reply({ content: `Purchase ticket: <#${channel.id}>`, ephemeral: true });
      }
    }

    // Modals
    if (interaction.isModalSubmit()) {
      if (!ensureAllowedGuild(interaction)) return;

      if (interaction.customId.startsWith("refmodal:")) {
        const channelId = interaction.customId.split(":")[1];
        if (interaction.channelId !== channelId) return interaction.reply({ content: "Wrong channel.", ephemeral: true });

        const ticket = await ticketsCol.findOne({ guildId: interaction.guild.id, channelId, status: "open", kind: "purchase" });
        if (!ticket) return interaction.reply({ content: "Not a valid open purchase ticket.", ephemeral: true });
        if (ticket.userId !== interaction.user.id) return interaction.reply({ content: "Only the ticket owner can set a reference code.", ephemeral: true });

        const entered = interaction.fields.getTextInputValue("refcode").trim().toLowerCase();
        if (!VALID_REFERENCE_CODES.has(entered)) return interaction.reply({ content: "Invalid reference code.", ephemeral: true });

        await ticketsCol.updateOne({ _id: ticket._id }, { $set: { referenceCode: entered, referenceSetAt: new Date() } }).catch(() => {});

        const refCh = await getReferenceLogChannel(interaction.guild);
        if (refCh) {
          const pendingEmbed = new EmbedBuilder()
            .setColor(BRAND_COLOR)
            .setTitle(`${BRAND_NAME} • Reference Code Set`)
            .addFields(
              { name: "Code", value: `\`${entered}\``, inline: true },
              { name: "User", value: `<@${interaction.user.id}>`, inline: true },
              { name: "Status", value: "`set (unpaid)`", inline: true },
              { name: "Ticket", value: `<#${channelId}>`, inline: true }
            )
            .setFooter({ text: BRAND_FOOTER })
            .setTimestamp(new Date());

          await refCh.send({ embeds: [pendingEmbed] }).catch(() => {});
        }

        return interaction.reply({ content: `✅ Reference code set: \`${entered}\``, ephemeral: true });
      }
    }

    // Buttons
    if (interaction.isButton()) {
      if (!ensureAllowedGuild(interaction)) return;

      if (interaction.customId === "support:open") {
        if (isCooling(`supportopen:${interaction.user.id}`)) return interaction.reply({ content: "Slow down.", ephemeral: true });

        const { channel, ticket } = await createOrGetTicket({ guild: interaction.guild, userId: interaction.user.id, kind: "support" });
        const embed = buildSupportTicketEmbed({ userId: interaction.user.id });
        const row = supportUtilityButtons();

        const msg = await channel.send({ content: `<@${interaction.user.id}>`, embeds: [embed], components: row }).catch(() => null);
        if (msg?.id) await ticketsCol.updateOne({ _id: ticket._id }, { $set: { ticketPanelMessageId: msg.id } }).catch(() => {});

        return interaction.reply({ content: `Support ticket: <#${channel.id}>`, ephemeral: true });
      }

      if (interaction.customId === "ticket:close") {
        const ticket = await ticketsCol.findOne({
          guildId: interaction.guild.id,
          channelId: interaction.channelId,
          status: "open",
        });

        if (!ticket) return interaction.reply({ content: "Not a valid open ticket channel.", ephemeral: true });

        const allowed = await canManageTicket(interaction, ticket);
        if (!allowed) return interaction.reply({ content: "No permission.", ephemeral: true });

        await interaction.deferReply({ ephemeral: true }).catch(() => {});
        await closeAndDeleteTicketChannel({ interaction, ticketDoc: ticket, reason: "Ticket closed" });
        return;
      }

      if (interaction.customId === "ref:add") {
        const ticket = await ticketsCol.findOne({ guildId: interaction.guild.id, channelId: interaction.channelId, status: "open", kind: "purchase" });
        if (!ticket) return interaction.reply({ content: "Not a purchase ticket.", ephemeral: true });
        if (ticket.userId !== interaction.user.id) return interaction.reply({ content: "Only ticket owner can set a reference code.", ephemeral: true });

        const modal = new ModalBuilder()
          .setCustomId(`refmodal:${interaction.channelId}`)
          .setTitle(`${BRAND_NAME} • Reference Code`);

        const input = new TextInputBuilder()
          .setCustomId("refcode")
          .setLabel("Enter reference code")
          .setStyle(TextInputStyle.Short)
          .setPlaceholder("e.g. synex")
          .setRequired(true)
          .setMaxLength(32);

        modal.addComponents(new ActionRowBuilder().addComponents(input));
        return interaction.showModal(modal);
      }

      if (interaction.customId.startsWith("buy:")) {
        if (isCooling(`buy:${interaction.user.id}`)) return interaction.reply({ content: "Slow down.", ephemeral: true });

        const [, productId, planKey] = interaction.customId.split(":");
        if (!ObjectId.isValid(productId)) return interaction.reply({ content: "Invalid product.", ephemeral: true });
        if (!PLAN_KEYS.includes(planKey)) return interaction.reply({ content: "Invalid plan.", ephemeral: true });

        const ticket = await ticketsCol.findOne({ guildId: interaction.guild.id, channelId: interaction.channelId, status: "open", kind: "purchase" });
        if (!ticket) return interaction.reply({ content: "Not a purchase ticket.", ephemeral: true });
        if (interaction.user.id !== ticket.userId) return interaction.reply({ content: "Only ticket owner can buy.", ephemeral: true });

        const product = await productsCol.findOne({ _id: new ObjectId(productId), guildId: interaction.guild.id });
        if (!product) return interaction.reply({ content: "Product not found.", ephemeral: true });
        if (!product.prices?.[planKey]?.amountMinor) return interaction.reply({ content: "That plan isn't enabled.", ephemeral: true });

        const gate = await canStartCheckout({
          guildId: interaction.guild.id,
          userId: interaction.user.id,
          product,
          planKey,
          isUpgrade: false,
        });
        if (!gate.ok) return interaction.reply({ content: gate.reason, ephemeral: true });

        const referenceCode = ticket.referenceCode && VALID_REFERENCE_CODES.has(ticket.referenceCode) ? ticket.referenceCode : null;

        const purchaseId = makePurchaseId();
        const url = await createCheckoutSessionForProduct({
          guildId: interaction.guild.id,
          userId: interaction.user.id,
          product,
          planKey,
          purchaseId,
          referenceCode,
          modeTag: "purchase",
        });

        await interaction.channel.send({
          embeds: [
            new EmbedBuilder()
              .setColor(BRAND_COLOR)
              .setTitle(`${BRAND_NAME} • Checkout Link`)
              .setDescription(
                `Purchase ID: \`${purchaseId}\`\n` +
                `Product: **${product.name}**\n` +
                `Plan: **${PLAN_LABELS[planKey]}**\n` +
                `Reference: ${referenceCode ? `\`${referenceCode}\`` : "`none`"}\n\n` +
                `Checkout: ${url}`
              )
              .setFooter({ text: BRAND_FOOTER })
              .setTimestamp(new Date()),
          ],
        }).catch(() => {});

        return interaction.reply({ content: "Checkout link posted.", ephemeral: true });
      }

      if (interaction.customId.startsWith("upgrade:")) {
        if (isCooling(`upgradebtn:${interaction.user.id}`)) return interaction.reply({ content: "Slow down.", ephemeral: true });

        const [, productId, targetPlan] = interaction.customId.split(":");
        if (!ObjectId.isValid(productId)) return interaction.reply({ content: "Invalid product.", ephemeral: true });
        if (!(targetPlan === "monthly" || targetPlan === "annual")) return interaction.reply({ content: "Invalid upgrade target.", ephemeral: true });

        const ticket = await ticketsCol.findOne({ guildId: interaction.guild.id, channelId: interaction.channelId, status: "open", kind: "purchase" });
        if (!ticket) return interaction.reply({ content: "Not a purchase ticket.", ephemeral: true });
        if (interaction.user.id !== ticket.userId) return interaction.reply({ content: "Only ticket owner can upgrade.", ephemeral: true });

        const product = await productsCol.findOne({ _id: new ObjectId(productId), guildId: interaction.guild.id });
        if (!product) return interaction.reply({ content: "Product not found.", ephemeral: true });
        if (!product.prices?.[targetPlan]?.amountMinor) return interaction.reply({ content: "That subscription plan isn't enabled.", ephemeral: true });

        const ent = await getEntitlement(interaction.guild.id, interaction.user.id, productId);
        if (!ent || ent.status !== "active" || ent.planKey !== "one_time") {
          return interaction.reply({ content: "Upgrade is only for users who own **One-time**.", ephemeral: true });
        }

        const gate = await canStartCheckout({
          guildId: interaction.guild.id,
          userId: interaction.user.id,
          product,
          planKey: targetPlan,
          isUpgrade: true,
        });
        if (!gate.ok) return interaction.reply({ content: gate.reason, ephemeral: true });

        const purchaseId = makePurchaseId();
        const url = await createCheckoutSessionForProduct({
          guildId: interaction.guild.id,
          userId: interaction.user.id,
          product,
          planKey: targetPlan,
          purchaseId,
          referenceCode: ent.referenceCode || null,
          modeTag: "upgrade",
        });

        await interaction.channel.send({
          embeds: [
            new EmbedBuilder()
              .setColor(BRAND_COLOR)
              .setTitle(`${BRAND_NAME} • Upgrade Checkout`)
              .setDescription(
                `Purchase ID: \`${purchaseId}\`\n` +
                `Product: **${product.name}**\n` +
                `Upgrade to: **${PLAN_LABELS[targetPlan]}**\n\n` +
                `Checkout: ${url}`
              )
              .setFooter({ text: BRAND_FOOTER })
              .setTimestamp(new Date()),
          ],
        }).catch(() => {});

        return interaction.reply({ content: "Upgrade link posted.", ephemeral: true });
      }

      if (interaction.customId.startsWith("refund_approve:") || interaction.customId.startsWith("refund_reject:")) {
        const [action, requestId] = interaction.customId.split(":");

        const isApprover = interaction.user.id === REFUND_APPROVER_USER_ID || isAdmin(interaction);
        if (!isApprover) return interaction.reply({ content: "Not allowed.", ephemeral: true });

        await interaction.deferReply({ ephemeral: true }).catch(() => {});

        const rr = await refundRequestsCol.findOne({ requestId, guildId: interaction.guild.id });
        if (!rr) return interaction.editReply("Refund request not found.");
        if (rr.status !== "pending") return interaction.editReply(`Refund request is already \`${rr.status}\`.`);

        const purchase = await purchasesCol.findOne({ purchaseId: rr.purchaseId, guildId: interaction.guild.id });
        if (!purchase) {
          await refundRequestsCol.updateOne({ requestId }, { $set: { status: "rejected", rejectedAt: new Date(), rejectedBy: interaction.user.id, reason: "Purchase missing" } });
          return interaction.editReply("Purchase missing. Request rejected.");
        }

        if (action === "refund_reject") {
          await refundRequestsCol.updateOne(
            { requestId },
            { $set: { status: "rejected", rejectedAt: new Date(), rejectedBy: interaction.user.id } }
          );
          return interaction.editReply("Refund rejected.");
        }

        const paidAt = purchase.paidAt ? new Date(purchase.paidAt) : null;
        if (!paidAt) return interaction.editReply("No paidAt. Unsafe to refund.");
        if (Date.now() - paidAt.getTime() > REFUND_WINDOW_MS) return interaction.editReply("Refund window expired (>24h).");

        try {
          const refundId = await executeRefundInternal(purchase);

          await refundRequestsCol.updateOne(
            { requestId },
            { $set: { status: "approved", approvedAt: new Date(), approvedBy: interaction.user.id, stripeRefundId: refundId } }
          );

          await purchasesCol.updateOne(
            { purchaseId: purchase.purchaseId },
            { $set: { status: "refunded", refundedAt: new Date(), stripeRefundId: refundId } }
          );

          if (purchase.productId) {
            await entitlementsCol.updateOne(
              { guildId: purchase.guildId, userId: purchase.userId, productId: purchase.productId },
              { $set: { status: "revoked", revokedAt: new Date(), revokedBy: interaction.user.id } }
            ).catch(() => {});
          }

          return interaction.editReply(`Refund approved. Refund ID: \`${refundId || "n/a"}\``);
        } catch (e) {
          console.error("Refund execute failed:", e);
          return interaction.editReply("Refund failed in Stripe.");
        }
      }
    }
  } catch (e) {
    console.error(e);
    if (interaction.isRepliable()) {
      interaction.reply({ content: "Something broke. Shocking.", ephemeral: true }).catch(() => {});
    }
  }
});

// ===================== READY =====================
client.on("ready", async () => {
  console.log(`🤖 Logged in as ${client.user.tag}`);
  const guild = await client.guilds.fetch(ALLOWED_GUILD_ID).catch(() => null);
  if (!guild) return;

  await ensurePurchaseLogChannel(guild).catch(() => {});
  await upsertPanels(guild).catch(() => {});
});

// ===================== WEB SERVER =====================
const app = express();
app.disable("x-powered-by");
app.set("trust proxy", 1);

app.get("/health", (req, res) => res.status(200).send("ok"));
app.get("/success", (req, res) => res.status(200).send("Payment successful. You can close this tab."));
app.get("/cancel", (req, res) => res.status(200).send("Payment canceled."));

app.post("/stripe/webhook", express.raw({ type: "application/json" }), async (req, res) => {
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, req.headers["stripe-signature"], STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error("❌ Webhook signature failed:", err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  try {
    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const meta = session.metadata || {};

      const purchaseId = meta.purchase_id;
      const guildId = meta.guild_id;
      const userId = meta.user_id;
      const type = meta.type;

      if (!purchaseId || !guildId || !userId || !type) return res.json({ received: true });
      if (guildId !== ALLOWED_GUILD_ID) return res.json({ received: true });

      const paymentIntentId = session.payment_intent || null;
      const subscriptionId = session.subscription || null;

      let currentPeriodEnd = null;
      let subscriptionStatus = null;
      let cancelAtPeriodEnd = null;

      if (subscriptionId) {
        const sub = await stripe.subscriptions.retrieve(subscriptionId);
        currentPeriodEnd = sub.current_period_end ? new Date(sub.current_period_end * 1000) : null;
        subscriptionStatus = sub.status;
        cancelAtPeriodEnd = !!sub.cancel_at_period_end;
      }

      await purchasesCol.updateOne(
        { purchaseId, stripeSessionId: session.id },
        {
          $setOnInsert: {
            purchaseId,
            guildId,
            userId,
            type,
            amountMinor: Number(meta.amount_minor || 0) || 0,
            currency: meta.currency || CURRENCY,
            referenceCode: meta.reference_code && meta.reference_code !== "none" ? meta.reference_code : null,
            stripeSessionId: session.id,
            createdAt: new Date(),
          },
          $set: {
            status: "paid",
            paidAt: new Date(),
            stripePaymentIntentId: paymentIntentId,
            stripeSubscriptionId: subscriptionId,
            currentPeriodEnd,
            subscriptionStatus,
            cancelAtPeriodEnd,
            planKey: meta.plan_key || null,
            productId: meta.product_id || null,
          },
        },
        { upsert: true }
      );

      const guild = await client.guilds.fetch(guildId).catch(() => null);
      if (!guild) return res.json({ received: true });

      if (type === "product" && meta.role_id) {
        try {
          const member = await guild.members.fetch(userId);
          await member.roles.add(meta.role_id, `Stripe paid: ${purchaseId}`);
        } catch (e) {
          console.error("Role grant failed:", e);
        }
      }

      if (type === "product" && meta.product_id) {
        const productId = meta.product_id;
        const planKey = meta.plan_key || "one_time";
        const referenceCode = meta.reference_code && meta.reference_code !== "none" ? meta.reference_code : null;

        await entitlementsCol.updateOne(
          { guildId, userId, productId },
          {
            $setOnInsert: { createdAt: new Date() },
            $set: {
              guildId,
              userId,
              productId,
              status: "active",
              planKey,
              referenceCode,
              lastPurchaseId: purchaseId,
              stripeSubscriptionId: subscriptionId || null,
              currentPeriodEnd: currentPeriodEnd || null,
              subscriptionStatus: subscriptionStatus || null,
              updatedAt: new Date(),
            },
          },
          { upsert: true }
        );

        // DM one-time buyers with monthly upgrade hint
        try {
          const product = await productsCol.findOne({ _id: new ObjectId(productId), guildId });
          if (product && planKey === "one_time" && product.prices?.monthly?.amountMinor) {
            await dmUser(userId, {
              content:
                `✅ Payment received for **${product.name}** (One-time).\n\n` +
                `If you want **updates + new versions**, upgrade to **Monthly**: **${minorToDisplay(product.prices.monthly.amountMinor)}/month**.\n` +
                `Use **/upgrade product_id:${productId} to:monthly**`,
            });
          }
        } catch {}
      }

      const amountMinor = Number(meta.amount_minor || 0) || 0;
      const planKey = meta.plan_key || null;

      const logEmbed = new EmbedBuilder()
        .setColor(BRAND_COLOR)
        .setTitle(`${BRAND_NAME} • Purchase Completed`)
        .addFields(
          { name: "User", value: `<@${userId}>`, inline: true },
          { name: "Purchase ID", value: `\`${purchaseId}\``, inline: true },
          { name: "Type", value: type, inline: true },
          { name: "Amount", value: amountMinor ? minorToDisplay(amountMinor) : "n/a", inline: true },
          ...(planKey ? [{ name: "Plan", value: PLAN_LABELS[planKey] || planKey, inline: true }] : []),
          ...(meta.product_name ? [{ name: "Product", value: meta.product_name, inline: false }] : []),
          ...(meta.mode_tag ? [{ name: "Mode", value: meta.mode_tag, inline: true }] : [])
        )
        .setFooter({ text: BRAND_FOOTER })
        .setTimestamp(new Date());

      await logToPurchaseLog(guild, logEmbed);
      return res.json({ received: true });
    }

    return res.json({ received: true });
  } catch (err) {
    console.error("Webhook handler error:", err);
    return res.status(500).send("Webhook handler failed");
  }
});

// ===================== START =====================
async function main() {
  await initDb();

  try {
    await registerCommands();
  } catch (e) {
    console.error("Command registration failed:", e);
  }

  await client.login(DISCORD_TOKEN);

  const server = app.listen(PORT, "0.0.0.0", () => console.log(`🌐 Web server on :${PORT}`));

  const shutdown = async (signal) => {
    console.log(`🧯 ${signal} received. Shutting down...`);
    try { server.close(); } catch {}
    try { await client.destroy(); } catch {}
    try { await mongo.close(); } catch {}
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

main().catch((e) => {
  console.error("Fatal startup error:", e);
  process.exit(1);
});
